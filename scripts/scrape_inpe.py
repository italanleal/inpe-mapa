#!/usr/bin/env python3
"""Scraper avulso do INPE: baixa CSVs de 10 min, consolida em ordem cronológica e envia ao S3."""
import argparse
import csv
import hashlib
import json
import logging
import os
from pathlib import Path
import re
import shutil
import sqlite3
import sys
import time
from html.parser import HTMLParser
from urllib.parse import urljoin
from urllib.request import Request, urlopen

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common.env import load_dotenv
from common.records import from_csv, key

SOURCE = 'https://dataserver-coids.inpe.br/queimadas/queimadas/focos/csv/10min/'
DEFAULT_S3_KEY = 'consolidated/dataset.csv'
LOG = logging.getLogger(__name__)


class Listing(HTMLParser):
    def __init__(self):
        super().__init__()
        self.files = set()

    def handle_starttag(self, tag, attrs):
        href = dict(attrs).get('href', '')
        if tag == 'a' and re.fullmatch(r'focos_10min_\d{8}_\d{4}\.csv', href):
            self.files.add(href)


def fetch(url, target=None):
    for attempt in range(4):
        try:
            with urlopen(Request(url, headers={'User-Agent': 'INPE-H3-Educational/1.0'}), timeout=90) as response:
                if target is None:
                    return response.read().decode('utf-8-sig')
                temporary = target.with_suffix('.part')
                with temporary.open('wb') as output:
                    while chunk := response.read(1024 * 1024):
                        output.write(chunk)
                temporary.replace(target)
                return
        except Exception:
            if attempt == 3:
                raise
            time.sleep(2 ** attempt)


def connect(path):
    db = sqlite3.connect(path)
    db.execute('CREATE TABLE IF NOT EXISTS records (id TEXT PRIMARY KEY, date TEXT NOT NULL, payload TEXT NOT NULL)')
    db.execute('CREATE INDEX IF NOT EXISTS ordered ON records(date,id)')
    db.execute('CREATE TABLE IF NOT EXISTS files (name TEXT PRIMARY KEY)')
    db.commit()
    return db


def ingest(db, path, brazil_only=True):
    """Uma transação por arquivo; falhas não marcam o arquivo como concluído."""
    with path.open(encoding='utf-8-sig', newline='') as source:
        sample = source.read(8192)
        source.seek(0)
        # Arquivos vazios e avisos de ausência de focos não são datasets.
        if not sample.strip():
            return 0
        if '\n' not in sample.strip() and any(s in key(sample) for s in ('semdados', 'semfocos', 'nodata', 'norecords', 'nenhumfoco')):
            return 0
        delimiter = ';' if sample.splitlines()[0].count(';') > sample.splitlines()[0].count(',') else ','
        reader = csv.DictReader(source, delimiter=delimiter)
        columns = {key(c) for c in (reader.fieldnames or [])}
        required = [{'datahoragmt', 'datahora', 'data', 'datetime', 'timestamp'},
                    {'satelite', 'satellite'}, {'lat', 'latitude'}, {'lon', 'long', 'longitude'}]
        if any(not columns.intersection(group) for group in required):
            raise ValueError(f'{path.name}: cabeçalho CSV não reconhecido: {reader.fieldnames}')
        if brazil_only and not columns.intersection({'pais', 'country'}):
            LOG.warning('%s: sem coluna de país; serão preservadas todas as observações, inclusive fora do Brasil', path.name)
        added = 0
        with db:
            for line, row in enumerate(reader, 2):
                try:
                    item = from_csv(row, brazil_only)
                except (ValueError, TypeError) as error:
                    raise ValueError(f'{path.name}, linha {line}: {error}') from error
                if item:
                    result = db.execute('INSERT OR IGNORE INTO records VALUES (?,?,?)',
                                        (item['id'], item['date'], json.dumps(item, ensure_ascii=False)))
                    added += result.rowcount
        return added


def consolidate(db, directory):
    path = directory / 'consolidated.csv'
    temporary = directory / 'consolidated.tmp'
    fields = ['id', 'date', 'satellite', 'latitude', 'longitude']
    with temporary.open('w', encoding='utf-8', newline='') as output:
        writer = csv.DictWriter(output, fieldnames=fields)
        writer.writeheader()
        for (payload,) in db.execute('SELECT payload FROM records ORDER BY date,id'):
            writer.writerow(json.loads(payload))
    temporary.replace(path)
    with path.open('rb') as source:
        digest = hashlib.file_digest(source, 'sha256').hexdigest()
    return path, digest


def upload_dataset(s3, bucket, object_key, path, digest):
    s3.upload_file(
        str(path),
        bucket,
        object_key,
        ExtraArgs={'ContentType': 'text/csv; charset=utf-8', 'Metadata': {'sha256': digest}},
    )
    LOG.info('Dataset enviado: s3://%s/%s (sha256=%s)', bucket, object_key, digest)
    return {'bucket': bucket, 'key': object_key, 'sha256': digest}


def scrape_dataset(data_dir, db, max_files=0, all_countries=False):
    parser = Listing()
    parser.feed(fetch(SOURCE))
    names = sorted(parser.files)
    if not names:
        raise RuntimeError('A listagem do INPE não retornou arquivos CSV reconhecidos')
    if max_files:
        names = names[-max_files:]
    for name in names:
        if db.execute('SELECT 1 FROM files WHERE name=?', (name,)).fetchone():
            continue
        path = data_dir / 'raw' / name
        if not path.exists():
            fetch(urljoin(SOURCE, name), path)
        added = ingest(db, path, not all_countries)
        with db:
            db.execute('INSERT INTO files VALUES (?)', (name,))
        LOG.info('%s: %s novos registros', name, added)
    path, digest = consolidate(db, data_dir)
    LOG.info('Dataset consolidado: %s (sha256=%s)', path, digest)
    return path, digest


def reset_scraper_data(directory):
    directory = Path(directory).resolve()
    if not directory.exists():
        return []
    targets = [
        directory / 'raw',
        directory / 'consolidated.csv',
        directory / 'consolidated.tmp',
        directory / 'scraper.sqlite3',
        directory / 'scraper.sqlite3-wal',
        directory / 'scraper.sqlite3-shm',
        directory / 'scraper.sqlite3-journal',
    ]
    removed = []
    for target in targets:
        if target.is_symlink():
            target.unlink()
        elif target.is_dir():
            shutil.rmtree(target)
        elif target.exists():
            target.unlink()
        else:
            continue
        removed.append(str(target))
    return removed


def s3_client():
    import boto3
    from botocore.config import Config
    return boto3.client('s3', config=Config(retries={'mode': 'standard', 'max_attempts': 8}))


def main(argv=None):
    load_dotenv()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--bucket', default=os.getenv('S3_BUCKET'), help='Bucket S3 de destino (ou variável S3_BUCKET)')
    parser.add_argument('--s3-key', default=os.getenv('S3_KEY', DEFAULT_S3_KEY),
                        help=f'Chave do objeto no S3 (padrão: {DEFAULT_S3_KEY} ou variável S3_KEY)')
    parser.add_argument('--data-dir', type=Path, default=Path(os.getenv('DATA_DIR', 'data')),
                        help='Diretório local temporário para downloads e consolidação')
    parser.add_argument('--max-files', type=int, default=0,
                        help='0: todos os arquivos disponíveis; N: os N mais recentes')
    parser.add_argument('--all-countries', action='store_true',
                        help='Desabilita o filtro por Brasil quando existir coluna pais/country')
    parser.add_argument('--prepare-only', action='store_true',
                        help='Somente download e consolidação local, sem upload ao S3')
    parser.add_argument('--reset', action='store_true',
                        help='Apaga downloads, consolidação e banco local do scraper antes de executar')
    args = parser.parse_args(argv)
    if args.max_files < 0:
        parser.error('--max-files deve ser >= 0')
    if not args.prepare_only and not args.bucket:
        parser.error('Defina S3_BUCKET ou --bucket (ou use --prepare-only para execução local)')
    if not args.s3_key or args.s3_key.startswith('/'):
        parser.error('--s3-key deve ser uma chave S3 válida sem barra inicial')
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    if args.reset:
        removed = reset_scraper_data(args.data_dir)
        LOG.info('Reset local do scraper concluído: %s artefatos removidos', len(removed))
    args.data_dir.mkdir(parents=True, exist_ok=True)
    (args.data_dir / 'raw').mkdir(exist_ok=True)
    db = connect(args.data_dir / 'scraper.sqlite3')
    try:
        path, digest = scrape_dataset(args.data_dir, db, max_files=args.max_files, all_countries=args.all_countries)
        if not args.prepare_only:
            upload_dataset(s3_client(), args.bucket, args.s3_key, path, digest)
    finally:
        db.close()
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
