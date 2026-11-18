import datetime as dt
import json
import unittest

import od_dong_export as m


def stat(day, n=10, r=100.0, origins=426):
    return {'day_type': day, 'n': n, 'r': r, 'origins': origins}


class FakeBlob:
    def __init__(self, store, name):
        self.store, self.name = store, name

    def download_as_bytes(self):
        return self.store[self.name]

    def upload_from_string(self, data, content_type=None):
        self.store[self.name] = data.encode() if isinstance(data, str) else data

    def delete(self):
        self.store.pop(self.name, None)


class FakeStorage:
    def __init__(self, store):
        self.store = store

    def list_blobs(self, bucket, prefix=None):
        return [FakeBlob(self.store, n) for n in sorted(self.store) if n.startswith(prefix or '')]

    def bucket(self, name):
        store = self.store

        class B:
            def blob(self, n):
                return FakeBlob(store, n)
        return B()


class FakeJob:
    def __init__(self, rows):
        self.rows = rows

    def result(self):
        return self.rows


class Row(dict):
    pass


class FakeBq:
    class QueryJobConfig:
        def __init__(self, **kw):
            pass


class FakeClient:
    def __init__(self, stats, store):
        self.stats, self.store, self.sql = stats, store, []

    def query(self, sql, job_config=None, location=None):
        self.sql.append(sql)
        if sql.startswith('EXPORT DATA'):
            folder = sql.split("uri='gs://b/", 1)[1].split('/part-', 1)[0]
            self.store[f'{folder}/part-000000000000.csv.gz'] = b'x'
            return FakeJob([])
        return FakeJob([Row(s) for s in self.stats])


class Args:
    project, dataset, out_uri, dry_run = 'p', 'transit_marts', 'gs://b/od-dong', False


class OdDongExportTest(unittest.TestCase):
    def test_sql_names_the_dataset_and_folds_outside(self):
        sql = m.export_sql('p', 'ds')
        self.assertIn('`p.ds`.fct_od_dong_mode_typical', sql)
        self.assertIn("COALESCE(o.adm_cd, 'outside')", sql)

    def test_stats_need_every_day_type_and_enough_codes(self):
        ok = m.check_stats([stat('weekday'), stat('saturday'), stat('sunday_holiday')])
        self.assertEqual(ok['weekday'], {'rows': 10, 'total_rides': 100.0, 'seoul_origins': 426})
        with self.assertRaises(ValueError):
            m.check_stats([stat('weekday'), stat('saturday')])
        with self.assertRaises(ValueError):
            m.check_stats([stat('weekday'), stat('saturday'), stat('sunday_holiday', origins=300)])

    def test_exports_then_skips_the_same_numbers(self):
        store = {}
        stats = [stat('weekday'), stat('saturday'), stat('sunday_holiday')]
        client = FakeClient(stats, store)
        m.run(FakeStorage(store), client, FakeBq, Args(), today=dt.date(2026, 11, 18))
        manifest = json.loads(store['od-dong/20261118/manifest.json'])
        self.assertEqual(manifest['parts'], ['part-000000000000.csv.gz'])
        self.assertEqual(manifest['columns'], m.COLUMNS)
        self.assertTrue(any(s.startswith('EXPORT DATA') for s in client.sql))

        again = FakeClient(stats, store)
        m.run(FakeStorage(store), again, FakeBq, Args(), today=dt.date(2026, 11, 25))
        self.assertFalse(any(s.startswith('EXPORT DATA') for s in again.sql))
        self.assertNotIn('od-dong/20261125/manifest.json', store)

    def test_new_numbers_export_again(self):
        store = {}
        m.run(FakeStorage(store), FakeClient([stat('weekday'), stat('saturday'), stat('sunday_holiday')], store),
              FakeBq, Args(), today=dt.date(2026, 11, 18))
        changed = FakeClient([stat('weekday', r=101.0), stat('saturday'), stat('sunday_holiday')], store)
        m.run(FakeStorage(store), changed, FakeBq, Args(), today=dt.date(2026, 12, 15))
        self.assertTrue(any(s.startswith('EXPORT DATA') for s in changed.sql))


if __name__ == '__main__':
    unittest.main()
