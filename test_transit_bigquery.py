"""Run offline checks without real PostgreSQL or GCP calls."""
import tempfile
import unittest
from pathlib import Path
from datetime import datetime,timezone
from types import SimpleNamespace
from unittest.mock import patch
import pyarrow.parquet as pq
import transit_bigquery as app

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

    def test_existing_table_refuses_any_load(self):
        with tempfile.TemporaryDirectory() as d:
            args=SimpleNamespace(folder=d,project=app.PROJECT,dataset=app.DATASET)
            m={'snapshot_id':'a'*32,'tables':{}}
            calls=[]
            def fake(args,cmd):
                calls.append(cmd)
                if cmd[0]=='show':return {'location':app.LOCATION}
                return [{'tableReference':{'tableId':'bus_stop_location'}}]
            with patch.object(app,'read_manifest',return_value=m),patch.object(app,'bq_call',side_effect=fake):
                with self.assertRaisesRegex(ValueError,'refusing'):app.load(args)
            self.assertFalse(any('load' in c for c in calls))

    def test_incomplete_export_refused(self):
        with tempfile.TemporaryDirectory() as d:
            app.save_json(Path(d)/'manifest.json',{'complete':False})
            with self.assertRaisesRegex(ValueError,'incomplete'):app.read_manifest(Path(d))

if __name__=='__main__':unittest.main()
