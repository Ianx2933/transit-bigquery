"""od_load.py without Cloud Storage or BigQuery: both are replaced by fakes.

Needs no Google packages, so it runs anywhere pytest or unittest does.
"""
import json
import unittest
from types import SimpleNamespace

import od_load as app


def report(day, rows=10, pax=100, arows=1, apax=2):
    return {'day': day, 'structural': {'rows': {'input': rows + arows, 'output': rows, 'anomaly': arows},
                                       'passengers': {'input': pax + apax, 'output': pax, 'anomaly': apax}}}


class Blob:
    def __init__(self, name, data):
        self.name, self._data = name, data

    def download_as_bytes(self):
        return self._data


class FakeStorage:
    def __init__(self, reports, prefix='od-curated'):
        self.blobs = [Blob(f'{prefix}/{r["day"]}/report.json', json.dumps(r).encode()) for r in reports]
        self.blobs.append(Blob(f'{prefix}/{reports[0]["day"]}/curated.csv.gz', b''))
        self.blobs.append(Blob(f'{prefix}/_inputs/stop_master_x.csv', b''))

    def list_blobs(self, bucket, prefix):
        assert bucket == 'seoul-transit-prod-pipeline'
        return [b for b in self.blobs if b.name.startswith(prefix)]


class Rows:
    def __init__(self, rows):
        self.rows = rows

    def result(self):
        return [SimpleNamespace(items=lambda r=r: r.items()) for r in self.rows]


class FakeBigQuery:
    """Partitions are {table: {day: (rows, passengers, lo, hi)}}. A load copies the report's numbers."""
    def __init__(self, reports, wrong=None, conflict=(), failed_jobs=()):
        self.reports = {r['day']: r for r in reports}
        self.partitions = {t: {} for t in app.TABLES}
        self.created, self.loads, self.attached = [], [], []
        self.wrong = wrong or {}            # (table, day) -> stats to store instead
        self.conflict = set(conflict)       # (table, day) whose first submission conflicts
        self.tables = {}                    # table_id -> SimpleNamespace(time_partitioning, clustering_fields, num_rows)
        self.deleted = []
        self.failed_jobs = set(failed_jobs)  # (table, day) whose existing job ended in error
        self.job_ids = []

    def get_table(self, table_id):
        if table_id not in self.tables:
            raise type('NotFound', (Exception,), {})(table_id)
        return self.tables[table_id]

    def delete_table(self, table_id):
        self.deleted.append(table_id)
        del self.tables[table_id]

    def create_table(self, table, exists_ok=False):
        self.created.append(table.table_id)
        self.tables[table.table_id] = SimpleNamespace(
            time_partitioning=SimpleNamespace(field=None), clustering_fields=getattr(table, 'clustering_fields', None),
            num_rows=0)

    def query(self, sql, job_config):
        table = sql.split('.')[-1].split('`')[0]
        return Rows([{'day': d, 'n': s[0], 'p': s[1], 'lo': s[2], 'hi': s[3]}
                     for d, s in self.partitions[table].items() if s[0]])  # GROUP BY has no empty days

    def _apply(self, table, day):
        kind = app.TABLES[table][1]
        n, p = app.expected(self.reports[day], kind)
        self.partitions[table][day] = self.wrong.get((table, day), (n, p, day[2:], day[2:]))

    def load_table_from_uri(self, uri, target, job_id, job_config, location):
        table, day = target.split('.')[-1].split('$')
        assert job_config.write_disposition == 'WRITE_TRUNCATE'
        self.job_ids.append(job_id)
        base_id = job_id[:len(f'od_{table}_{day}_') + 16]
        if (table, day) in self.failed_jobs and job_id == base_id:
            raise type('Conflict', (Exception,), {})('job exists')
        # The table's partitioning and clustering apply; stating them risks a mismatch.
        assert not hasattr(job_config, 'time_partitioning') and not hasattr(job_config, 'clustering_fields')
        assert uri.endswith(f'/{day}/{app.TABLES[table][0]}')
        if (table, day) in self.conflict:
            self.conflict.discard((table, day))
            self._apply(table, day)
            raise type('Conflict', (Exception,), {})('job exists')
        self.loads.append((table, day))
        self._apply(table, day)
        return SimpleNamespace(result=lambda: None)

    def get_job(self, job_id, location):
        self.attached.append(job_id)
        for table, day in self.failed_jobs:
            if job_id.startswith(f'od_{table}_{day}_'):
                return SimpleNamespace(state='DONE', error_result={'reason': 'invalid'},
                                       result=lambda: (_ for _ in ()).throw(RuntimeError('old failure')))
        return SimpleNamespace(state='DONE', error_result=None, result=lambda: None)


class Namespace(SimpleNamespace):
    pass


FAKE_BQ = SimpleNamespace(
    SchemaField=lambda n, t: (n, t),
    Table=lambda tid, schema: SimpleNamespace(table_id=tid, schema=schema),
    TimePartitioning=lambda type_: ('DAY', type_),
    TimePartitioningType=SimpleNamespace(DAY='DAY'),
    QueryJobConfig=lambda **k: k,
    LoadJobConfig=lambda **k: SimpleNamespace(**k),
    SourceFormat=SimpleNamespace(CSV='CSV'),
    WriteDisposition=SimpleNamespace(WRITE_TRUNCATE='WRITE_TRUNCATE'),
)


def args(**k):
    base = dict(curated_uri='gs://seoul-transit-prod-pipeline/od-curated', project=app.PROJECT,
                dataset=app.DATASET, only_day=None, dry_run=False, check_existing=False)
    base.update(k)
    return SimpleNamespace(**base)


REPORTS = [report('20260915'), report('20260919', rows=8, pax=80, arows=0, apax=0)]


class OdLoadTests(unittest.TestCase):
    def run_app(self, bq, **k):
        app.run(FakeStorage(REPORTS), bq, FAKE_BQ, args(**k))

    def test_every_day_is_loaded_into_its_partition_and_verified(self):
        bq = FakeBigQuery(REPORTS)
        self.run_app(bq)
        # 20260919 has no anomalies, so there is nothing to load for it in that table
        self.assertEqual(sorted(bq.loads), sorted((t, d) for t in app.TABLES for d in ('20260915', '20260919')
                                                  if (t, d) != ('od_route_stop_anomaly', '20260919')))
        self.assertEqual(len(bq.created), 2)

    def test_second_run_loads_nothing(self):
        bq = FakeBigQuery(REPORTS)
        self.run_app(bq)
        bq.loads.clear()
        self.run_app(bq)
        self.assertEqual(bq.loads, [])

    def test_only_day_reloads_even_when_it_matches(self):
        bq = FakeBigQuery(REPORTS)
        self.run_app(bq)
        bq.loads.clear()
        self.run_app(bq, only_day='20260919')
        self.assertEqual(sorted(bq.loads), [('od_route_stop', '20260919'), ('od_route_stop_anomaly', '20260919')])

    def test_a_partition_that_does_not_match_its_report_stops_the_run(self):
        bq = FakeBigQuery(REPORTS, wrong={('od_route_stop', '20260915'): (10, 99, '260915', '260915')})
        with self.assertRaisesRegex(ValueError, '20260915: 10 rows / 99 passengers'):
            self.run_app(bq)

    def test_rows_from_another_date_in_a_partition_stop_the_run(self):
        bq = FakeBigQuery(REPORTS, wrong={('od_route_stop', '20260915'): (10, 100, '260914', '260915')})
        with self.assertRaisesRegex(ValueError, 'source dates'):
            self.run_app(bq)

    def test_retried_submission_attaches_to_the_first_job(self):
        bq = FakeBigQuery(REPORTS, conflict={('od_route_stop', '20260915')})
        self.run_app(bq)
        self.assertEqual(len(bq.attached), 1)
        self.assertTrue(bq.attached[0].startswith('od_od_route_stop_20260915_'))

    def test_a_failed_earlier_job_is_not_reused(self):
        bq = FakeBigQuery(REPORTS, failed_jobs={('od_route_stop', '20260915')})
        self.run_app(bq)                     # would raise 'old failure' if it attached
        retried = [j for j in bq.job_ids if j.startswith('od_od_route_stop_20260915_')]
        self.assertEqual(len(retried), 2)
        self.assertNotEqual(retried[0], retried[1])
        self.assertIn(('od_route_stop', '20260915'), bq.loads)

    def test_a_partition_with_blank_dates_is_reloaded(self):
        bq = FakeBigQuery(REPORTS)
        self.run_app(bq)
        n, p, _, _ = bq.partitions['od_route_stop']['20260915']
        bq.partitions['od_route_stop']['20260915'] = (n, p, None, None)
        bq.loads.clear()
        self.run_app(bq)
        self.assertEqual(bq.loads, [('od_route_stop', '20260915')])

    def test_dry_run_changes_nothing(self):
        bq = FakeBigQuery(REPORTS)
        self.run_app(bq, dry_run=True)
        self.assertEqual((bq.loads, bq.created), ([], []))

    def test_empty_clustered_table_is_recreated_and_a_full_one_is_refused(self):
        bq = FakeBigQuery(REPORTS)
        tid = f'{app.PROJECT}.{app.DATASET}.od_route_stop'
        bq.tables[tid] = SimpleNamespace(time_partitioning=SimpleNamespace(field=None),
                                         clustering_fields=['route_name', 'board_ars'], num_rows=0)
        self.run_app(bq)
        self.assertEqual(bq.deleted, [tid])
        self.assertIsNone(bq.tables[tid].clustering_fields)
        bq2 = FakeBigQuery(REPORTS)
        bq2.tables[tid] = SimpleNamespace(time_partitioning=SimpleNamespace(field=None),
                                          clustering_fields=['route_name'], num_rows=5)
        with self.assertRaisesRegex(ValueError, 'not dropped automatically'):
            self.run_app(bq2)
        self.assertEqual(bq2.deleted, [])

    def test_existing_tables_are_left_alone(self):
        bq = FakeBigQuery(REPORTS)
        self.run_app(bq)
        created = len(bq.created)
        self.run_app(bq)
        self.assertEqual((len(bq.created), bq.deleted), (created, []))

    def test_empty_anomaly_day_needs_no_partition(self):
        stats = {}
        self.assertEqual(app.check(stats, {'20260919': (REPORTS[1], 'x')}, 'od_route_stop_anomaly', 'anomaly'), [])
        self.assertTrue(app.check(stats, {'20260919': (REPORTS[1], 'x')}, 'od_route_stop', 'output'))

    def test_reports_outside_day_folders_are_ignored(self):
        found = app.read_reports(FakeStorage(REPORTS), 'gs://seoul-transit-prod-pipeline/od-curated')
        self.assertEqual(sorted(found), ['20260915', '20260919'])


if __name__ == '__main__':
    unittest.main()
