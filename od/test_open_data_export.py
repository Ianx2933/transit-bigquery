import gzip
import json
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parent))
import open_data_export as app  # noqa: E402


class Blob:
    def __init__(self, store, name):
        self.store, self.name = store, name
        self.content_type = self.content_disposition = None

    @property
    def size(self):
        return len(self.store.objects[self.name])

    def exists(self):
        return self.name in self.store.objects

    def download_as_bytes(self):
        return self.store.objects[self.name]

    def upload_from_string(self, data, content_type=None):
        self.store.objects[self.name] = data.encode() if isinstance(data, str) else data
        self.store.order.append(self.name)

    def compose(self, sources):
        self.store.objects[self.name] = b''.join(self.store.objects[b.name] for b in sources)
        self.store.order.append(self.name)

    def patch(self):
        self.store.meta[self.name] = (self.content_type, self.content_disposition)

    def reload(self):
        pass

    def delete(self):
        del self.store.objects[self.name]


class Storage:
    def __init__(self):
        self.objects, self.order, self.meta = {}, [], {}

    def bucket(self, name):
        def copy_blob(blob, bucket, target):
            self.objects[target] = self.objects[blob.name]
            self.order.append(target)
        return SimpleNamespace(blob=lambda n: Blob(self, n), get_blob=lambda n: Blob(self, n),
                               copy_blob=copy_blob)

    def list_blobs(self, bucket, prefix=None):
        return [Blob(self, n) for n in sorted(self.objects) if n.startswith(prefix or '')]


class Rows(list):
    def result(self):
        return self


class Job:
    def result(self):
        return self


class BigQuery:
    """Window query answers from rows; table queries are recorded; extracts write shards."""

    def __init__(self, storage, shards=2, num_rows=5):
        self.storage, self.shards, self.num_rows = storage, shards, num_rows
        self.tables, self.extracts, self.dry_runs = [], [], []

    def query(self, sql, job_config=None, location=None):
        if getattr(job_config, 'dry_run', False):
            self.dry_runs.append(sql)
            return SimpleNamespace(total_bytes_processed=1_000_000)
        if getattr(job_config, 'destination', None):
            self.tables.append(job_config.destination)
            return Job()
        return Rows([SimpleNamespace(items=lambda: {'day_type': 'weekday', 'n': 10, 'p': 1.0,
                                                    'end_day': '20261020', 'start_day': '20251021'}.items())])

    def get_table(self, table_id):
        return SimpleNamespace(num_rows=self.num_rows,
                               schema=[SimpleNamespace(name='a'), SimpleNamespace(name='b')])

    def extract_table(self, table_id, dest, job_id, job_config, location):
        self.extracts.append((table_id, dest, job_config.print_header))
        base = dest[len('gs://seoul-transit-open-data/'):]
        for i in range(self.shards):
            self.storage.objects[base.replace('*', f'{i:012d}')] = gzip.compress(f'{i},x\n'.encode())
        return Job()


class Cfg(SimpleNamespace):
    pass


BQ = SimpleNamespace(QueryJobConfig=Cfg, ExtractJobConfig=Cfg,
                     DestinationFormat=SimpleNamespace(CSV='CSV'), Compression=SimpleNamespace(GZIP='GZIP'))


def args(**kw):
    base = dict(project='p', source='transit_marts', open_dataset='transit_open',
                out_uri='gs://seoul-transit-open-data', dry_run=False)
    base.update(kw)
    return SimpleNamespace(**base)


class OpenDataExportTest(unittest.TestCase):
    def setUp(self):
        self.storage = Storage()
        self.bq = BigQuery(self.storage)

    def test_writes_one_file_per_table_with_header_then_manifest_last(self):
        app.run(self.storage, self.bq, BQ, args())
        names = [d[0] for d in app.datasets('p', 'transit_marts')]
        self.assertEqual(self.bq.tables, [f'p.transit_open.{n}' for n in names])
        release = 'releases/20261020'
        for n in names:
            text = gzip.decompress(self.storage.objects[f'{release}/{n}.csv.gz']).decode()
            self.assertEqual(text, 'a,b\n0,x\n1,x\n')
            self.assertIn(f'{n}_20261020.csv.gz', self.storage.meta[f'{release}/{n}.csv.gz'][1])
        self.assertFalse(any('/_tmp/' in k for k in self.storage.objects))
        release_writes = [o for o in self.storage.order if o.startswith(release)]
        self.assertTrue(release_writes[-1].endswith('manifest.json'))
        manifest = json.loads(self.storage.objects[f'{release}/manifest.json'])
        self.assertEqual([f['name'] for f in manifest['files']], names)
        self.assertIn('공공누리', manifest['license'])
        self.assertEqual(manifest['window_end'], '20261020')

    def test_latest_mirrors_release_and_drops_stale_files(self):
        self.storage.objects['latest/old.csv.gz'] = b'x'
        app.run(self.storage, self.bq, BQ, args())
        latest = sorted(k for k in self.storage.objects if k.startswith('latest/'))
        self.assertNotIn('latest/old.csv.gz', latest)
        self.assertIn('latest/manifest.json', latest)
        self.assertEqual(len(latest), len(app.datasets('p', 'x')) + 1)
        latest_writes = [o for o in self.storage.order if o.startswith('latest/')]
        self.assertTrue(latest_writes[-1].endswith('manifest.json'))

    def test_same_rows_skip_extract(self):
        app.run(self.storage, self.bq, BQ, args())
        first = len(self.bq.extracts)
        app.run(self.storage, self.bq, BQ, args())
        self.assertEqual(len(self.bq.extracts), first)

    def test_changed_rows_export_again(self):
        app.run(self.storage, self.bq, BQ, args())
        self.bq.num_rows = 7
        app.run(self.storage, self.bq, BQ, args())
        manifest = json.loads(self.storage.objects['releases/20261020/manifest.json'])
        self.assertEqual({f['rows'] for f in manifest['files']}, {7})

    def test_many_shards_compose_in_rounds(self):
        self.bq.shards = 70
        app.run(self.storage, self.bq, BQ, args())
        text = gzip.decompress(self.storage.objects['releases/20261020/stops.csv.gz']).decode().splitlines()
        self.assertEqual(text[0], 'a,b')
        self.assertEqual(len(text), 71)
        self.assertEqual(text[1:], [f'{i},x' for i in range(70)])
        self.assertFalse(any('.join' in k for k in self.storage.objects))

    def test_dry_run_writes_nothing(self):
        app.run(self.storage, self.bq, BQ, args(dry_run=True))
        self.assertEqual(self.storage.objects, {})
        self.assertEqual(self.bq.tables, [])
        self.assertEqual(len(self.bq.dry_runs), len(app.datasets('p', 'x')))


if __name__ == '__main__':
    unittest.main()
