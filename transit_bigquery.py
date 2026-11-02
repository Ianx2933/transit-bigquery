"""Export a read-only PostgreSQL snapshot and load it into BigQuery staging.

This is the ingestion half: a faithful 1:1 copy of thirteen source tables into
`transit_staging`, with a manifest, per-file checksums and an idempotent load
record. The dimensional model is built from that copy by the dbt project
(transit_dbt/); this script does no transformation.

Requires Python 3.14+. Run --help for available commands.

No scheduler, GCS, DML, or PostgreSQL writes are used.
"""
import argparse
import getpass
import hashlib
import json
import math
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import uuid
from datetime import datetime, timezone

# Supplied live schema, 2026-09-08. * means PostgreSQL NOT NULL.

SPECS = {
 'integrated_hourly_transit_demand_light': 'mode:s* service_id:s* node_id:s* node_name:s day_type:s* hour:i* boarding:f alighting:f source:s',
 'bus_stop_demand_node_mapping': 'service_id:s* service_id_type:s route_name:s converted_route_id:s demand_node_id:s* canonical_node_id:s* ars_id:s stop_name:s node_info_stop_name:s lat:f lng:f region_id:s seq:s side:s* match_method:s match_confidence:f reference_rows:i passenger_sum:i canonical_candidate_count:i node_info_ars_candidate_count:f',
 'integrated_bus_stop_location': 'canonical_node_id:s* ars_id:s stop_name:s node_info_stop_name:s lat:f lng:f region_id:s source_region:s source_file:s match_method:s match_confidence:f geom:g',
 'bus_stop_location': '노드id:s* 정류장번호:s 정류장명:s 경도:f 위도:f 표준코드여부:i geom:g elev_m:f slope_pct:f',
 'subway_station_location': 'station_id:s* station_name:s line_name:s lat:f lng:f',
 'subway_station_join_alias': 'source_line_name:s* source_station_name:s* target_line_name:s target_station_name:s reason:s',
 'admin_dong_boundary': 'ogc_fid:i* base_date:s adm_cd:s adm_nm:s geom:g',
 'terrain_dataset': 'dataset_id:s* source_id:s* slope_method:s* boundary_base_date:s* processed_at:t* published_at:t stop_count:i* measured_stop_count:i* assigned_stop_count:i* ambiguous_boundary_stop_count:i*',
 'bus_stop_terrain_stats': 'dataset_id:s* node_id:s* stop_no:s stop_name:s* geom:g* elev_m:f slope_pct:f',
 'terrain_stop_dong_assignment': 'dataset_id:s* node_id:s* adm_cd:s adm_nm:s assignment_method:s* candidate_count:i*',
 'terrain_active_dataset': 'singleton:b* dataset_id:s* activated_at:t*',
 'month_day_count': 'use_ym:s* mon:i tue:i wed:i thu:i fri:i sat:i sun_holiday:i',
 # Raw source of fact_boarding / dim_route / dim_stop. Loaded untransformed;
 # renames, casts and joins live in dbt models, not here.
 'hourly_bus_stop_passenger': 'use_ym:s route_no:s route_name:s stop_id:s stop_ars:s stop_name:s hour:i boarding_passengers:f alighting_passengers:f',
}
HOURLY = 'hourly_bus_stop_passenger'
ALIASES = {'노드id':'node_id','정류장번호':'stop_no','정류장명':'stop_name',
           '경도':'lng','위도':'lat','표준코드여부':'standard_code_flag'}
PROJECT = 'project-9c77d932-f0d2-47bb-947'

# Raw tables land in staging, not in the warehouse dataset. transit_warehouse
# holds the dimensional model (fact_boarding, dim_stop, dim_route) built from
# this staging copy; mixing a 1:1 source replica into it would leave a reader
# unable to tell the modelled tables from the copied ones.
DATASET = 'transit_staging'
LOCATION = 'asia-northeast3'


def fields(table):
    return [(n, t.rstrip('*'), not t.endswith('*'))
            for n,t in (x.split(':') for x in SPECS[table].split())]


def quote(name):
    return '"' + name.replace('"','""') + '"'


def export_schema(table):
    import pyarrow as pa
    types = {'s':pa.string(),'g':pa.string(),'i':pa.int64(),
             'f':pa.float64(),'b':pa.bool_(),'t':pa.timestamp('us',tz='UTC')}
    cols, expressions = [], []
    for name, typ, nullable in fields(table):
        output = ALIASES.get(name,name) if table == 'bus_stop_location' else name
        if typ == 'g':
            # An unknown SRID raises an error; no CRS is assigned implicitly.
            output = name + '_wkt'
            expr = f'ST_AsText(ST_Force2D(ST_Transform({quote(name)},4326)),17)'
        else:
            expr = quote(name)
        cols.append(pa.field(output, types[typ], nullable=nullable))
        expressions.append(expr + ' AS ' + quote(output))
    cols.extend([pa.field('export_snapshot_id',pa.string(),False),
                 pa.field('exported_at',pa.timestamp('us',tz='UTC'),False)])
    return pa.schema(cols), ', '.join(expressions)


def to_arrow(rows, schema, snapshot, exported_at):
    import pyarrow as pa
    arrays = []
    for i,field in enumerate(list(schema)[:-2]):
        values = [r[i] for r in rows]
        if not field.nullable and any(v is None for v in values):
            raise ValueError(f'Unexpected NULL in {field.name}')
        if pa.types.is_floating(field.type):
            if any(v is not None and not math.isfinite(v) for v in values):
                raise ValueError(f'Non-finite number in {field.name}')
        arrays.append(pa.array(values,type=field.type))
    arrays.extend([pa.array([snapshot]*len(rows),type=pa.string()),
                   pa.array([exported_at]*len(rows),type=schema[-1].type)])
    return pa.Table.from_arrays(arrays,schema=schema)


def digest(path):
    h=hashlib.sha256()
    with path.open('rb') as f:
        for chunk in iter(lambda:f.read(1024*1024),b''): h.update(chunk)
    return h.hexdigest()


def save_json(path,obj):
    tmp=path.with_suffix(path.suffix+'.tmp')
    tmp.write_text(json.dumps(obj,ensure_ascii=False,indent=2,allow_nan=False),encoding='utf-8')
    tmp.replace(path)


def export(args):
    import psycopg2
    import pyarrow.parquet as pq
    out=Path(args.out).resolve()
    if out.exists(): raise ValueError('Output folder already exists. Choose a NEW --out folder; old exports are preserved.')
    password=getpass.getpass('PostgreSQL password (hidden): ')
    conn=psycopg2.connect(host=args.host,port=args.port,user=args.user,
                          dbname=args.dbname,password=password,connect_timeout=15)
    del password
    snapshot=uuid.uuid4().hex
    now=datetime.now(timezone.utc)
    manifest={'format_version':1,'snapshot_id':snapshot,'exported_at':now.isoformat(),
              'source_period':None,'source_period_note':'Not inferred from export date. Confirm upstream input period before time-series comparisons.',
              'geometry':'geom_wkt is 2D EPSG:4326 WKT; not yet BigQuery GEOGRAPHY',
              'tables':{},'demand_totals':[],'hourly_totals':[],'complete':False}
    try:
        conn.set_session(isolation_level='REPEATABLE READ',readonly=True,autocommit=False)
        with conn.cursor() as cur:
            cur.execute("SET TIME ZONE 'UTC'")
            # Check all schema types before creating any export files.
            for table in SPECS:
                cur.execute('SELECT column_name,udt_name FROM information_schema.columns WHERE table_schema=%s AND table_name=%s',('public',table))
                actual=dict(cur.fetchall())
                allowed={'s':{'varchar','text'},'i':{'int2','int4','int8'},'f':{'float4','float8'},'b':{'bool'},'t':{'timestamptz'},'g':{'geometry'}}
                for name,typ,_ in fields(table):
                    if actual.get(name) not in allowed[typ]:
                        raise ValueError(f'Schema mismatch: {table}.{name}: {actual.get(name)}')
            cur.execute('SELECT mode,day_type,COUNT(*),SUM(boarding),SUM(alighting) FROM public.integrated_hourly_transit_demand_light GROUP BY mode,day_type ORDER BY mode,day_type')
            manifest['demand_totals']=[dict(zip(['mode','day_type','row_count','boarding','alighting'],r)) for r in cur.fetchall()]
            # Per-month control totals for the raw fact source, checked again after load.
            cur.execute(f'SELECT use_ym,COUNT(*),SUM(boarding_passengers),SUM(alighting_passengers),COUNT(DISTINCT stop_ars) FROM public.{quote(HOURLY)} GROUP BY use_ym ORDER BY use_ym')
            manifest['hourly_totals']=[dict(zip(['use_ym','row_count','boarding','alighting','distinct_stops'],r)) for r in cur.fetchall()]
        out.mkdir(parents=True)
        for table in SPECS:
            schema,select=export_schema(table)
            path=out/(table+'.parquet')
            with conn.cursor() as cur:
                cur.execute(f'SELECT COUNT(*) FROM public.{quote(table)}')
                expected=cur.fetchone()[0]
            count=0
            with conn.cursor(name='export_'+table) as cur, pq.ParquetWriter(path,schema,compression='snappy',version='1.0') as writer:
                cur.itersize=args.chunk_rows
                cur.execute(f'SELECT {select} FROM public.{quote(table)}')
                while True:
                    rows=cur.fetchmany(args.chunk_rows)
                    if not rows: break
                    writer.write_table(to_arrow(rows,schema,snapshot,now),row_group_size=args.chunk_rows)
                    count+=len(rows)
                    print(f'{table}: {count:,}/{expected:,}',flush=True)
            if count!=expected or pq.ParquetFile(path).metadata.num_rows!=expected:
                raise ValueError(f'Export count mismatch: {table}')
            manifest['tables'][table]={'file':path.name,'rows':count,'bytes':path.stat().st_size,'sha256':digest(path),
                'columns':[{'name':f.name,'type':str(f.type),'nullable':f.nullable} for f in schema]}
            save_json(out/'manifest.json',manifest)
            print(f'OK {table}: {count:,} rows, {path.stat().st_size/1024/1024:.2f} MiB',flush=True)
        if not manifest['demand_totals']: raise ValueError('Demand table is empty; inspect source before loading.')
        if not manifest['hourly_totals']: raise ValueError(HOURLY+' is empty; inspect source before loading.')
        manifest['complete']=True
        save_json(out/'manifest.json',manifest)
        print(f'EXPORT COMPLETE: {out}\nRead-only PostgreSQL snapshot; no cloud upload yet.')
    finally:
        conn.rollback()
        conn.close()


def bq_call(args,tail):
    exe=shutil.which('bq') or shutil.which('bq.cmd')
    if not exe: raise ValueError('bq not found. Use Google Cloud SDK Shell or reopen your terminal.')
    command=[exe,'--project_id='+args.project,'--location='+LOCATION,'--format=prettyjson']+tail
    # Windows .cmd wrappers interpret metacharacters; reject unsafe paths and arguments.
    if os.name=='nt' and any(any(c in arg for c in '\r\n&|<>^%!') for arg in command):
        raise ValueError('Windows shell metacharacter in argument/path. Move files to a simple path such as C:\\transit_bigquery.')
    env=os.environ.copy(); env['PYTHONIOENCODING']='utf-8'
    result=subprocess.run(command,capture_output=True,text=True,encoding='utf-8',errors='replace',env=env)
    if result.returncode:
        raise RuntimeError((result.stderr+'\n'+result.stdout).strip())
    if 'load' in tail: return None
    return json.loads(result.stdout) if result.stdout.strip() else None


def read_manifest(folder):
    m=json.loads((folder/'manifest.json').read_text(encoding='utf-8'))
    if not m.get('complete') or set(m['tables'])!=set(SPECS):
        raise ValueError('Export incomplete; do not load. Export again to a NEW folder.')
    if not re.fullmatch('[a-f0-9]{32}',m['snapshot_id']): raise ValueError('Invalid snapshot ID')
    for table,entry in m['tables'].items():
        if entry['file']!=table+'.parquet': raise ValueError('Unexpected manifest filename')
        if digest(folder/entry['file'])!=entry['sha256']: raise ValueError(f'File checksum mismatch: {table}')
    return m


def load(args):
    folder=Path(args.folder).resolve(); m=read_manifest(folder)
    ds=bq_call(args,['show','--dataset=true',args.project+':'+args.dataset])
    if ds.get('location','').lower()!=LOCATION: raise ValueError('Dataset must be in '+LOCATION)
    state_path=folder/'load_state.json'
    state=json.loads(state_path.read_text()) if state_path.exists() else {'snapshot_id':m['snapshot_id'],'project':args.project,'dataset':args.dataset,'loaded':{}}
    if (state['snapshot_id'],state['project'],state['dataset'])!=(m['snapshot_id'],args.project,args.dataset):
        raise ValueError('This folder has load state for a different target/snapshot.')
    existing=bq_call(args,['ls','--max_results=10000',args.project+':'+args.dataset]) or []
    names={t['tableReference']['tableId'] for t in existing}
    collisions=(names & set(SPECS))-set(state['loaded'])
    if collisions: raise ValueError('Existing tables have no successful load record for this export; refusing to append/overwrite: '+', '.join(sorted(collisions)))
    for table,entry in m['tables'].items():
        target=args.project+':'+args.dataset+'.'+table
        if table in state['loaded']:
            meta=bq_call(args,['show',target])
            if str(meta.get('id'))!=state['loaded'][table]['id'] or str(meta.get('creationTime'))!=state['loaded'][table]['creationTime'] or str(meta.get('lastModifiedTime'))!=state['loaded'][table]['lastModifiedTime'] or int(meta['numRows'])!=entry['rows']:
                raise ValueError('Previously loaded table changed: '+table)
            print('SKIP verified previous load: '+table,flush=True)
            continue
        job='stp_'+m['snapshot_id']+'_'+table
        # Load each local file atomically into one table; use a fixed job ID to prevent duplicate submission.
        command=['--job_id='+job,'load','--source_format=PARQUET']
        if table=='integrated_hourly_transit_demand_light': command+=['--clustering_fields=mode,day_type,service_id,node_id']
        if table==HOURLY: command+=['--clustering_fields=use_ym,route_no,stop_ars']
        command += [target,str(folder/entry['file'])]
        print('LOAD '+table,flush=True)
        bq_call(args,command)
        meta=bq_call(args,['show',target])
        if int(meta['numRows'])!=entry['rows']: raise ValueError('BigQuery row count mismatch: '+table)
        state['loaded'][table]={'id':str(meta['id']),'creationTime':str(meta['creationTime']),'lastModifiedTime':str(meta['lastModifiedTime'])}
        save_json(state_path,state)
        print(f'OK {table}: {entry["rows"]:,} rows',flush=True)
    verify(args,m)


def compare_totals(expected,actual):
    e={(r['mode'],r['day_type']):r for r in expected}
    a={(r['mode'],r['day_type']):r for r in actual}
    if set(e)!=set(a): raise ValueError('Demand group keys differ')
    for key,er in e.items():
        ar=a[key]
        if int(er['row_count'])!=int(ar['row_count']): raise ValueError(f'Count mismatch: {key}')
        for col in ('boarding','alighting'):
            if er[col] is None or ar[col] is None:
                if er[col]!=ar[col]: raise ValueError(f'NULL sum mismatch: {key} {col}')
            elif not math.isclose(float(er[col]),float(ar[col]),rel_tol=1e-8,abs_tol=1e-6):
                raise ValueError(f'Sum mismatch: {key} {col}')


def compare_monthly(expected,actual):
    if not expected: raise ValueError('Manifest has no hourly_totals; export again with this version.')
    e={str(r['use_ym']):r for r in expected}
    a={str(r['use_ym']):r for r in actual}
    if set(e)!=set(a): raise ValueError('Month keys differ: '+', '.join(sorted(set(e)^set(a))))
    for ym,er in e.items():
        ar=a[ym]
        for col in ('row_count','distinct_stops'):
            if int(er[col])!=int(ar[col]): raise ValueError(f'{col} mismatch: {ym}')
        for col in ('boarding','alighting'):
            if er[col] is None or ar[col] is None:
                if er[col]!=ar[col]: raise ValueError(f'NULL sum mismatch: {ym} {col}')
            elif not math.isclose(float(er[col]),float(ar[col]),rel_tol=1e-9,abs_tol=1e-6):
                raise ValueError(f'Sum mismatch: {ym} {col}')


def verify(args,m):
    ref=f'`{args.project}.{args.dataset}.integrated_hourly_transit_demand_light`'
    query=f'SELECT mode,day_type,COUNT(*) AS row_count,SUM(boarding) AS boarding,SUM(alighting) AS alighting FROM {ref} GROUP BY mode,day_type ORDER BY mode,day_type'
    result=bq_call(args,['query','--use_legacy_sql=false','--maximum_bytes_billed=2147483648','--max_rows=100',query])
    compare_totals(m['demand_totals'],result)
    ref=f'`{args.project}.{args.dataset}.{HOURLY}`'
    query=f'SELECT use_ym,COUNT(*) AS row_count,SUM(boarding_passengers) AS boarding,SUM(alighting_passengers) AS alighting,COUNT(DISTINCT stop_ars) AS distinct_stops FROM {ref} GROUP BY use_ym ORDER BY use_ym'
    result=bq_call(args,['query','--use_legacy_sql=false','--maximum_bytes_billed=4294967296','--max_rows=1000',query])
    compare_monthly(m.get('hourly_totals') or [],result)
    months=[r['use_ym'] for r in m['hourly_totals']]
    print(f'VERIFIED: all {len(SPECS)} staging table row counts during load; demand group totals match; '
          f'{HOURLY} matches per month ({len(months)} months, {months[0]}..{months[-1]}). Geometry remains WKT; joins are not yet applied.')


def main():
    p=argparse.ArgumentParser(description=__doc__)
    sub=p.add_subparsers(dest='action',required=True)
    e=sub.add_parser('export'); e.add_argument('--out',default='export_02')
    e.add_argument('--host',default='localhost'); e.add_argument('--port',type=int,default=5432)
    e.add_argument('--user',default='postgres'); e.add_argument('--dbname',default='Seoul_Transit')
    e.add_argument('--chunk-rows',type=int,default=50000)
    l=sub.add_parser('load'); l.add_argument('--folder',default='export_02')
    l.add_argument('--project',default=PROJECT); l.add_argument('--dataset',default=DATASET)
    args=p.parse_args()
    if args.action=='export':
        if not 1<=args.chunk_rows<=200000: p.error('--chunk-rows must be 1..200000')
        export(args)
    else:
        if not re.fullmatch('[a-z][a-z0-9-]{4,62}',args.project) or not re.fullmatch('[A-Za-z_][A-Za-z0-9_]*',args.dataset):
            p.error('Invalid project/dataset identifier')
        load(args)

if __name__=='__main__':
    try: main()
    except Exception as exc:
        print(f'STOP: {exc}',file=sys.stderr)
        sys.exit(1)
