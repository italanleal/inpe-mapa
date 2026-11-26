import csv
import io
import json
from pathlib import Path

import pytest

from common.records import from_csv, record, ten_minute_window, timestamp
from producer.main import stream_dataset
from scripts.scrape_inpe import (
    Listing,
    connect as connect_scraper,
    consolidate,
    ingest,
    upload_dataset,
)


def test_timezone_and_identity():
    assert timestamp('2026-11-10 01:30:00') == '2026-11-10T01:30:00.000000Z'
    assert record('2026-11-10T01:30:00Z', 'aqua', -10, -50) == record('2026-11-09T22:30:00-03:00', ' AQUA ', '-10.0', '-50')


@pytest.mark.parametrize('lat,lon', [(91, 0), (0, 181), ('nan', 0), (0, 'inf')])
def test_invalid_coordinates(lat, lon):
    with pytest.raises(ValueError):
        record('2026-11-10', 'AQUA', lat, lon)


def test_csv_schemas():
    a = from_csv({'DataHora': '2026-11-10 01:00:00', 'Satélite': 'AQUA', 'Latitude': '-10', 'Longitude': '-50', 'País': 'Brasil'})
    b = from_csv({'data_hora_gmt': '2026-11-10 01:00:00', 'satelite': 'AQUA', 'lat': '-10', 'lon': '-50', 'pais': 'Brasil'})
    assert a == b
    assert from_csv({'pais': 'Peru'}) is None


def test_listing_excludes_external_links():
    parser = Listing()
    parser.feed('<a href="focos_10min_20261110_0010.csv">x</a><a href="../bad.csv">bad</a><a href="https://evil/focos_10min_20261110_0010.csv">bad</a>')
    assert parser.files == {'focos_10min_20261110_0010.csv'}


def test_consolidation_sorted_deduplicated_and_atomic(tmp_path):
    db = connect_scraper(tmp_path / 'scraper.db')
    source = tmp_path / 'input.csv'
    source.write_text('DataHora,Satelite,Latitude,Longitude,Pais\n2026-11-10 02:00:00,AQUA,-10,-50,Brasil\n2026-11-10 01:00:00,TERRA,-11,-51,Brasil\n2026-11-10 02:00:00,AQUA,-10,-50,Brasil\n')
    assert ingest(db, source) == 2
    assert ingest(db, source) == 0
    path, digest = consolidate(db, tmp_path)
    with path.open() as stream:
        rows = list(csv.DictReader(stream))
    assert len(rows) == 2
    assert rows[0]['satellite'] == 'TERRA'
    assert len(digest) == 64
    source.write_text('DataHora,Satelite,Latitude,Longitude,Pais\n2026-11-11,AQUA,-10,-50,Brasil\n2026-11-11,AQUA,999,-50,Brasil\n')
    with pytest.raises(ValueError):
        ingest(db, source)
    assert db.execute('SELECT count(*) FROM records').fetchone()[0] == 2
    db.close()


def test_empty_and_invalid_header(tmp_path):
    db = connect_scraper(tmp_path / 'scraper.db')
    source = tmp_path / 'empty.csv'
    source.write_text('Nenhum foco encontrado')
    assert ingest(db, source) == 0
    source.write_text('<html>Erro no servidor</html>')
    with pytest.raises(ValueError, match='cabeçalho'):
        ingest(db, source)
    db.close()


class S3:
    def __init__(self):
        self.objects = {}

    def upload_file(self, path, bucket, key, **kwargs):
        self.objects[(bucket, key)] = Path(path).read_bytes()

    def get_object(self, Bucket, Key):
        if (Bucket, Key) not in self.objects:
            raise KeyError(f'Objeto não encontrado: s3://{Bucket}/{Key}')
        return {'Body': io.BytesIO(self.objects[(Bucket, Key)])}


class SQS:
    def __init__(self, fail=False):
        self.messages = []
        self.fail = fail

    def send_message(self, **kwargs):
        if self.fail:
            raise RuntimeError('Falha simulada')
        self.messages.append(json.loads(kwargs['MessageBody']))
        assert kwargs['MessageGroupId'] == 'inpe-chronological'


def test_stream_line_by_line_without_local_dedup(tmp_path):
    scraper_db = connect_scraper(tmp_path / 'scraper.db')
    with scraper_db:
        for i in range(5):
            item = record(f'2026-11-10T00:00:{i:02}Z', 'AQUA', -10, -50)
            scraper_db.execute('INSERT INTO records VALUES (?,?,?)', (item['id'], item['date'], json.dumps(item)))
    path, digest = consolidate(scraper_db, tmp_path)
    scraper_db.close()

    s3, sqs = S3(), SQS()
    upload_dataset(s3, 'bucket', 'consolidated/dataset.csv', path, digest)

    # Envia exatamente 1 registro por mensagem SQS, em ordem cronológica.
    assert stream_dataset(s3, sqs, 'bucket', 'queue.fifo', 'consolidated/dataset.csv', 0) == 5
    assert [len(m['records']) for m in sqs.messages] == [1, 1, 1, 1, 1]
    assert [m['records'][0]['date'] for m in sqs.messages] == [
        f'2026-11-10T00:00:{i:02}.000000Z' for i in range(5)
    ]

    # Sem deduplicação local no produtor: nova execução lê e envia o stream novamente.
    assert stream_dataset(s3, sqs, 'bucket', 'queue.fifo', 'consolidated/dataset.csv', 0) == 5
    assert len(sqs.messages) == 10


def test_stream_sends_each_line_before_reading_next():
    events = []
    header = b'id,date,satellite,latitude,longitude\n'
    rows = [
        b'id1,2026-11-10T00:00:01.000000Z,AQUA,-10,-50\n',
        b'id2,2026-11-10T00:00:02.000000Z,AQUA,-11,-51\n',
    ]

    class StepStream(io.RawIOBase):
        def __init__(self):
            self.chunks = [header] + rows
            self.index = 0

        def readable(self):
            return True

        def readinto(self, b):
            if self.index >= len(self.chunks):
                return 0
            chunk = self.chunks[self.index]
            self.index += 1
            events.append(f'read:{self.index - 1}')
            n = len(chunk)
            b[:n] = chunk
            return n

    class OrderedS3:
        def get_object(self, Bucket, Key):
            return {'Body': StepStream()}

    class OrderedSQS:
        def send_message(self, **kwargs):
            item_id = json.loads(kwargs['MessageBody'])['records'][0]['id']
            events.append(f'send:{item_id}')

    assert stream_dataset(OrderedS3(), OrderedSQS(), 'bucket', 'queue.fifo', 'consolidated/dataset.csv', 0) == 2
    assert events.index('send:id1') < events.index('read:2')
    assert events.index('read:2') < events.index('send:id2')


def test_stream_propagates_send_failure(tmp_path):
    scraper_db = connect_scraper(tmp_path / 'scraper.db')
    item = record('2026-11-10', 'AQUA', -10, -50)
    with scraper_db:
        scraper_db.execute('INSERT INTO records VALUES (?,?,?)', (item['id'], item['date'], json.dumps(item)))
    path, digest = consolidate(scraper_db, tmp_path)
    scraper_db.close()

    s3 = S3()
    upload_dataset(s3, 'bucket', 'consolidated/dataset.csv', path, digest)

    with pytest.raises(RuntimeError):
        stream_dataset(s3, SQS(fail=True), 'bucket', 'queue.fifo', 'consolidated/dataset.csv', 0)


def test_stream_rejects_invalid_csv_header():
    s3 = S3()
    s3.objects[('bucket', 'consolidated/bad.csv')] = b'col1,col2\n1,2\n'
    with pytest.raises(ValueError, match='colunas obrigatórias'):
        stream_dataset(s3, SQS(), 'bucket', 'queue.fifo', 'consolidated/bad.csv', 0)


@pytest.mark.parametrize('value,start,end', [
    ('2026-11-13T15:40:00Z', '2026-11-13T15:40:00Z', '2026-11-13T15:50:00Z'),
    ('2026-11-13T15:49:59.999999Z', '2026-11-13T15:40:00Z', '2026-11-13T15:50:00Z'),
    ('2026-11-13T15:50:00Z', '2026-11-13T15:50:00Z', '2026-11-13T16:00:00Z'),
    ('2026-11-13T23:59:59Z', '2026-11-13T23:50:00Z', '2026-11-14T00:00:00Z'),
    ('2026-11-13T23:59:59-03:00', '2026-11-14T02:50:00Z', '2026-11-14T03:00:00Z'),
])
def test_ten_minute_windows(value, start, end):
    assert ten_minute_window(value) == (start, end)


def test_inpe_four_column_csv_without_country(tmp_path, caplog):
    db = connect_scraper(tmp_path / 'scraper.db')
    source = tmp_path / 'focos_10min_20261113_2000.csv'
    source.write_text('lat,lon,satelite,data\n  -4.678900, -80.572000,GOES-19,2026-11-13 19:40:00\n  -4.697300, -80.681400,GOES-19,2026-11-13 19:40:00\n')
    assert ingest(db, source) == 2
    assert 'sem coluna de país' in caplog.text
    path, _ = consolidate(db, tmp_path)
    with path.open() as stream:
        rows = list(csv.DictReader(stream))
    assert rows[0]['date'] == '2026-11-13T19:40:00.000000Z'
    assert rows[0]['satellite'] == 'GOES-19'
    assert all(float(r['longitude']) < -80 for r in rows)
    assert ingest(db, source) == 0
    db.close()
