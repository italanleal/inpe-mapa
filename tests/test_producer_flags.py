import os

import pytest
import producer.main as producer
import scripts.scrape_inpe as scraper
from test_producer import S3, SQS


class TrackedS3(S3):
    def __init__(self):
        super().__init__()
        self.uploads = []

    def upload_file(self, path, bucket, key, **kwargs):
        self.uploads.append((bucket, key))
        super().upload_file(path, bucket, key, **kwargs)


@pytest.fixture
def pipeline(monkeypatch, tmp_path):
    names = [f'focos_10min_20261113_{hour:02}00.csv' for hour in range(8)]
    downloaded = []

    def fetch(url, target=None):
        if target is None:
            return ''.join(f'<a href="{name}">{name}</a>' for name in names)
        downloaded.append(target.name)
        hour = names.index(target.name)
        target.write_text(f'lat,lon,satelite,data\n-10,-50,AQUA,2026-11-13 {hour:02}:00:00\n')

    monkeypatch.setattr(scraper, 'fetch', fetch)
    s3, sqs = TrackedS3(), SQS()
    s3_client_calls = []
    producer_client_calls = []

    def fake_s3_client():
        s3_client_calls.append(True)
        return s3

    def fake_aws_clients():
        producer_client_calls.append(True)
        return s3, sqs

    monkeypatch.setattr(scraper, 's3_client', fake_s3_client)
    monkeypatch.setattr(producer, 'aws_clients', fake_aws_clients)
    scraper_dir = tmp_path / 'scraper-data'
    return scraper_dir, s3, sqs, downloaded, s3_client_calls, producer_client_calls


def test_standalone_scraper_uploads_fixed_key_without_sqs(pipeline):
    scraper_dir, s3, sqs, downloads, s3_calls, producer_calls = pipeline
    assert scraper.main(['--data-dir', str(scraper_dir), '--bucket', 'bucket', '--max-files', '6']) == 0
    assert len(downloads) == 6
    assert downloads[0] == 'focos_10min_20261113_0200.csv'
    assert s3.uploads == [('bucket', 'consolidated/dataset.csv')]
    assert len(s3_calls) == 1
    assert producer_calls == []
    assert sqs.messages == []


def test_scraper_custom_s3_key_and_producer_streams_line_by_line(pipeline):
    scraper_dir, s3, sqs, _, _, producer_calls = pipeline
    custom_key = 'consolidated/2026-11-13.csv'
    assert scraper.main(['--data-dir', str(scraper_dir), '--bucket', 'bucket', '--s3-key', custom_key, '--max-files', '6']) == 0
    assert s3.uploads == [('bucket', custom_key)]

    assert producer.main([
        '--start',
        '--bucket', 'bucket',
        '--s3-key', custom_key,
        '--queue-url', 'queue.fifo',
        '--read-interval-seconds', '0',
    ]) == 0
    assert len(producer_calls) == 1
    assert len(sqs.messages) == 6
    assert all(len(m['records']) == 1 for m in sqs.messages)


def test_scraper_prepare_only_is_offline_and_reset_cleans_local(pipeline):
    scraper_dir, _, _, _, s3_calls, producer_calls = pipeline
    assert scraper.main(['--data-dir', str(scraper_dir), '--prepare-only', '--max-files', '6']) == 0
    assert s3_calls == []
    assert (scraper_dir / 'consolidated.csv').exists()
    assert (scraper_dir / 'scraper.sqlite3').exists()

    assert scraper.main(['--data-dir', str(scraper_dir), '--prepare-only', '--reset', '--max-files', '2']) == 0
    assert s3_calls == []
    assert producer_calls == []


def test_producer_without_flags_prints_help_and_exits_zero(pipeline):
    _, _, _, _, _, producer_calls = pipeline
    assert producer.main([]) == 0
    assert producer_calls == []


def test_scraper_rejects_leading_slash_s3_key(pipeline):
    scraper_dir, _, _, _, _, _ = pipeline
    with pytest.raises(SystemExit):
        scraper.main(['--data-dir', str(scraper_dir), '--bucket', 'bucket', '--s3-key', '/consolidated/dataset.csv'])


def test_producer_rejects_leading_slash_s3_key(pipeline):
    with pytest.raises(SystemExit):
        producer.main([
            '--start',
            '--bucket', 'bucket',
            '--s3-key', '/consolidated/dataset.csv',
            '--queue-url', 'queue.fifo',
        ])


def test_load_dotenv_populates_missing_and_preserves_existing(tmp_path, monkeypatch):
    from common.env import load_dotenv
    env_file = tmp_path / '.env'
    env_file.write_text(
        '# comentário\n'
        'S3_BUCKET=bucket-from-env\n'
        'export S3_KEY="consolidated/custom.csv"\n'
        "SQS_QUEUE_URL='https://sqs.sa-east-1.amazonaws.com/123/fila.fifo'\n",
        encoding='utf-8',
    )
    monkeypatch.delenv('S3_BUCKET', raising=False)
    monkeypatch.setenv('S3_KEY', 'consolidated/override.csv')
    monkeypatch.delenv('SQS_QUEUE_URL', raising=False)

    assert load_dotenv(env_file) is True
    assert os.environ['S3_BUCKET'] == 'bucket-from-env'
    assert os.environ['S3_KEY'] == 'consolidated/override.csv'
    assert os.environ['SQS_QUEUE_URL'] == 'https://sqs.sa-east-1.amazonaws.com/123/fila.fifo'
    assert load_dotenv(tmp_path / 'missing.env') is False
