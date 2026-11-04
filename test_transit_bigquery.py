"""Run offline checks without real PostgreSQL or GCP calls.

BigQuery is replaced by FakeClient below, so these tests need the
google-cloud-bigquery package installed but make no network calls.
"""
import contextlib
import hashlib
import io
import json
import os
import tempfile
import unittest
from pathlib import Path
from datetime import datetime,timezone
from types import SimpleNamespace
from unittest.mock import MagicMock, patch
import pyarrow.parquet as pq
from google.api_core.exceptions import Conflict
import transit_bigquery as app


class FakeTable:
    def __init__(self,table_id,rows):
        self.table_id=table_id
        self.full_table_id=f'p:ds.{table_id}'
        self.num_rows=rows
        self.created=self.modified=datetime(2026,11,4,tzinfo=timezone.utc)


class FakeClient:
    """Just enough of bigquery.Client for load() and verify()."""
    def __init__(self,existing=(),rows=1,demand=None,hourly=None):
        self.existing=set(existing)
        self.rows=rows
        self.demand=demand or []
        self.hourly=hourly or []
        self.load_calls=[]          # (table, write_disposition, clustering_fields)
        self.conflict_on=set()      # tables whose first submission raises Conflict
        self.attached=[]            # job IDs fetched after a Conflict

    def get_dataset(self,ref): return SimpleNamespace(location=app.LOCATION)
    def list_tables(self,ds): return [SimpleNamespace(table_id=t) for t in sorted(self.existing)]
    def get_table(self,ref): return FakeTable(ref.split('.')[-1],self.rows)

    def load_table_from_file(self,f,target,job_id,job_config,location):
        table=target.split('.')[-1]
        if table in self.conflict_on:
            self.conflict_on.discard(table)
            raise Conflict('job already exists')
        self.load_calls.append((table,job_config.write_disposition,job_config.clustering_fields))
        return SimpleNamespace(result=lambda:None)

    def get_job(self,job_id,location):
        self.attached.append(job_id)
        return SimpleNamespace(result=lambda:None)

    def query(self,sql,job_config):
        res=self.demand if 'GROUP BY mode' in sql else self.hourly
        return SimpleNamespace(result=lambda:[SimpleNamespace(items=lambda r=r:r.items()) for r in res])


DEMAND=[dict(mode='bus',day_type='mon',row_count=2,boarding=3.0,alighting=4.0)]
HOURLY=[dict(use_ym='202401',row_count=5,boarding=10.0,alighting=9.0,distinct_stops=2)]


def make_export(folder,rows=1):
    """A complete export folder: one small file per table and a valid manifest."""
    m={'snapshot_id':'a'*32,'complete':True,'tables':{},'demand_totals':DEMAND,'hourly_totals':HOURLY}
    for t in app.SPECS:
        data=t.encode()
        (folder/(t+'.parquet')).write_bytes(data)
        m['tables'][t]={'file':t+'.parquet','rows':rows,'sha256':hashlib.sha256(data).hexdigest()}
    app.save_json(folder/'manifest.json',m)
    return m


def run_load(args):
    """Call app.load() without its per-table progress lines in the test output."""
    with contextlib.redirect_stdout(io.StringIO()):
        app.load(args)


def load_args(folder,replace=False):
    return SimpleNamespace(folder=str(folder),project=app.PROJECT,dataset=app.DATASET,replace=replace)


class TransferTests(unittest.TestCase):
    def test_roundtrip_all_schemas_with_empty_tables_nulls_and_precision(self):
        now=datetime(2026,9,8,1,2,3,123456,tzinfo=timezone.utc)
        values={'s':'00123_서울','i':9007199254740993,'f':6.5123456789,'b':True,'t':now,'g':'POINT (127.1 37.5)'}
        with tempfile.TemporaryDirectory() as d:
            for name in app.SPECS:
                schema,_=app.export_schema(name)
                row=tuple(values[t] for _,t,_ in app.fields(name))
                nullable=tuple(None if null else values[t] for _,t,null in app.fields(name))
                for rows in ([],[row,nullable]):
                    table=app.to_arrow(rows,schema,'a'*32,now)
                    path=Path(d)/(name+'.parquet')
                    pq.write_table(table,path,compression='snappy',version='1.0')
                    restored=pq.read_table(path)
                    self.assertEqual(restored.to_pylist(),table.to_pylist())
                    self.assertEqual(restored.num_rows,len(rows))
                if name=='bus_stop_location':
                    self.assertEqual(schema.names[0],'node_id')
                    self.assertIn('geom_wkt',schema.names)

    def test_invalid_values_rejected(self):
        schema,_=app.export_schema('integrated_hourly_transit_demand_light')
        now=datetime.now(timezone.utc)
        with self.assertRaisesRegex(ValueError,'NULL'):
            app.to_arrow([(None,'1','001','x','mon',0,1.,2.,'x')],schema,'a'*32,now)
        with self.assertRaisesRegex(ValueError,'Non-finite'):
            app.to_arrow([('bus','1','001','x','mon',0,float('nan'),2.,'x')],schema,'a'*32,now)

    def test_float_comparison_accepts_ordering_noise_rejects_real_change(self):
        e=[dict(mode='bus',day_type='mon',row_count=2,boarding=7517546.202123189,alighting=None)]
        a=[dict(e[0],row_count='2',boarding='7517546.202123195')]
        app.compare_totals(e,a)
        with self.assertRaises(ValueError): app.compare_totals(e,[dict(a[0],boarding=8000000)])
        with self.assertRaises(ValueError): app.compare_totals(e,[dict(a[0],row_count='3')])

    def test_monthly_comparison_catches_missing_month_and_changed_total(self):
        app.compare_monthly(HOURLY,[dict(HOURLY[0],row_count='5',distinct_stops='2')])
        with self.assertRaisesRegex(ValueError,'Month keys differ'):
            app.compare_monthly(HOURLY,[])
        with self.assertRaisesRegex(ValueError,'Sum mismatch'):
            app.compare_monthly(HOURLY,[dict(HOURLY[0],boarding=11.0)])
        with self.assertRaisesRegex(ValueError,'distinct_stops mismatch'):
            app.compare_monthly(HOURLY,[dict(HOURLY[0],distinct_stops=3)])

    def test_existing_table_refuses_any_load(self):
        # A staging table this export has no load record for must not be
        # touched unless --replace is passed, and nothing may be submitted.
        with tempfile.TemporaryDirectory() as d:
            m={'snapshot_id':'a'*32,'tables':{t:{'file':t+'.parquet','rows':1} for t in app.SPECS}}
            client=FakeClient(existing={'bus_stop_location'})
            with patch.object(app,'read_manifest',return_value=m),patch.object(app,'bq_client',return_value=client):
                with self.assertRaisesRegex(ValueError,'refusing'):run_load(load_args(d))
            self.assertEqual(client.load_calls,[])

    def test_replace_truncates_existing_tables(self):
        with tempfile.TemporaryDirectory() as d:
            make_export(Path(d))
            client=FakeClient(existing=set(app.SPECS),demand=DEMAND,hourly=HOURLY)
            with patch.object(app,'bq_client',return_value=client):
                run_load(load_args(d,replace=True))
            self.assertEqual(len(client.load_calls),len(app.SPECS))
            self.assertTrue(all(w=='WRITE_TRUNCATE' for _,w,_ in client.load_calls))
            clustering={t:c for t,_,c in client.load_calls}
            self.assertEqual(clustering[app.HOURLY],['use_ym','route_no','stop_ars'])

    def test_fresh_load_never_overwrites(self):
        with tempfile.TemporaryDirectory() as d:
            make_export(Path(d))
            client=FakeClient(demand=DEMAND,hourly=HOURLY)
            with patch.object(app,'bq_client',return_value=client):
                run_load(load_args(d))
            self.assertTrue(all(w=='WRITE_EMPTY' for _,w,_ in client.load_calls))

    def test_rerun_of_same_export_skips_loaded_tables(self):
        with tempfile.TemporaryDirectory() as d:
            make_export(Path(d))
            client=FakeClient(demand=DEMAND,hourly=HOURLY)
            with patch.object(app,'bq_client',return_value=client):
                run_load(load_args(d))
                client.load_calls.clear()
                client.existing=set(app.SPECS)
                run_load(load_args(d))
            self.assertEqual(client.load_calls,[])

    def test_rerun_detects_table_changed_since_load(self):
        with tempfile.TemporaryDirectory() as d:
            make_export(Path(d))
            client=FakeClient(demand=DEMAND,hourly=HOURLY)
            with patch.object(app,'bq_client',return_value=client):
                run_load(load_args(d))
                client.rows=2
                with self.assertRaisesRegex(ValueError,'changed'):run_load(load_args(d))

    def test_retried_submission_attaches_to_existing_job(self):
        # If a first attempt reached BigQuery, the retry must not load twice.
        with tempfile.TemporaryDirectory() as d:
            make_export(Path(d))
            client=FakeClient(demand=DEMAND,hourly=HOURLY)
            client.conflict_on={app.HOURLY}
            with patch.object(app,'bq_client',return_value=client):
                run_load(load_args(d))
            self.assertEqual(client.attached,['stp_'+'a'*32+'_'+app.HOURLY])
            self.assertNotIn(app.HOURLY,[t for t,_,_ in client.load_calls])

    def test_load_stops_when_totals_disagree(self):
        with tempfile.TemporaryDirectory() as d:
            make_export(Path(d))
            client=FakeClient(demand=DEMAND,hourly=[dict(HOURLY[0],boarding=99.0)])
            with patch.object(app,'bq_client',return_value=client):
                with self.assertRaisesRegex(ValueError,'Sum mismatch'):run_load(load_args(d))

    def test_incomplete_export_refused(self):
        with tempfile.TemporaryDirectory() as d:
            app.save_json(Path(d)/'manifest.json',{'complete':False})
            with self.assertRaisesRegex(ValueError,'incomplete'):app.read_manifest(Path(d))

    def test_existing_export_folder_refused_without_overwrite(self):
        with tempfile.TemporaryDirectory() as d:
            args=SimpleNamespace(out=d,overwrite=False)
            # psycopg2 is imported before the folder check; no connection is made.
            with patch.dict('sys.modules',{'psycopg2':MagicMock()}):
                with self.assertRaisesRegex(ValueError,'already exists'):app.export(args)


class PasswordTests(unittest.TestCase):
    def test_env_password_wins_without_prompt(self):
        with patch.dict(os.environ,{'PGPASSWORD':'x'}),patch('getpass.getpass') as prompt:
            self.assertIsNone(app.resolve_password())
            prompt.assert_not_called()

    def test_pgpass_file_used_without_prompt(self):
        with tempfile.NamedTemporaryFile(delete=False) as f: path=f.name
        try:
            env={k:v for k,v in os.environ.items() if k!='PGPASSWORD'}
            env['PGPASSFILE']=path
            with patch.dict(os.environ,env,clear=True),patch('getpass.getpass') as prompt:
                self.assertIsNone(app.resolve_password())
                prompt.assert_not_called()
        finally:
            os.unlink(path)

    def test_no_password_and_no_terminal_fails_instead_of_hanging(self):
        env={k:v for k,v in os.environ.items() if k!='PGPASSWORD'}
        env['PGPASSFILE']=str(Path(tempfile.gettempdir())/'no-such-pgpass')
        with patch.dict(os.environ,env,clear=True),patch('sys.stdin.isatty',return_value=False):
            with self.assertRaisesRegex(ValueError,'No PostgreSQL password'):app.resolve_password()


if __name__=='__main__':unittest.main()
