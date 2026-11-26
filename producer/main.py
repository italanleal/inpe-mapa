"""Produtor local: lê o dataset consolidado no S3 em stream linha a linha e envia ao SQS FIFO."""
import argparse
import csv
import io
import json
import logging
import os
import time

from common.env import load_dotenv

DEFAULT_S3_KEY = 'consolidated/dataset.csv'
REQUIRED_COLUMNS = {'id', 'date', 'satellite', 'latitude', 'longitude'}
LOG = logging.getLogger(__name__)


def stream_dataset(s3, sqs, bucket, queue_url, object_key, delay):
    if delay < 0:
        raise ValueError('Intervalo deve ser >= 0')
    response = s3.get_object(Bucket=bucket, Key=object_key)
    count = 0
    # Lê o CSV do S3 linha a linha em stream; envia cada linha ao SQS antes de ler a próxima.
    with io.TextIOWrapper(response['Body'], encoding='utf-8-sig', newline='') as stream:
        reader = csv.DictReader(stream)
        columns = set(reader.fieldnames or [])
        if not REQUIRED_COLUMNS.issubset(columns):
            raise ValueError(
                f'CSV consolidado no S3 sem colunas obrigatórias ({sorted(REQUIRED_COLUMNS)}): {reader.fieldnames}'
            )
        for row in reader:
            item = {
                'id': row['id'],
                'date': row['date'],
                'satellite': row['satellite'],
                'latitude': float(row['latitude']),
                'longitude': float(row['longitude']),
            }
            body = json.dumps({'schema_version': 1, 'records': [item]}, ensure_ascii=False, separators=(',', ':'))
            sqs.send_message(
                QueueUrl=queue_url,
                MessageBody=body,
                MessageGroupId='inpe-chronological',
                MessageDeduplicationId=item['id'],
            )
            count += 1
            LOG.info('Enviado registro %s (id=%s) de s3://%s/%s', count, item['id'][:12], bucket, object_key)
            if delay > 0:
                time.sleep(delay)
    return count


def aws_clients():
    import boto3
    from botocore.config import Config
    config = Config(retries={'mode': 'standard', 'max_attempts': 8})
    return boto3.client('s3', config=config), boto3.client('sqs', config=config)


def main(argv=None):
    load_dotenv()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--start', action='store_true',
                        help='Lê o dataset consolidado no S3 linha a linha e envia ao SQS FIFO')
    parser.add_argument('--bucket', default=os.getenv('S3_BUCKET'),
                        help='Bucket S3 com o dataset consolidado (ou variável S3_BUCKET)')
    parser.add_argument('--s3-key', default=os.getenv('S3_KEY', DEFAULT_S3_KEY),
                        help=f'Chave do CSV consolidado no S3 (padrão: {DEFAULT_S3_KEY} ou variável S3_KEY)')
    parser.add_argument('--queue-url', default=os.getenv('SQS_QUEUE_URL'),
                        help='URL da fila SQS FIFO (ou variável SQS_QUEUE_URL)')
    parser.add_argument('--read-interval-seconds', type=float, default=float(os.getenv('READ_INTERVAL_SECONDS', '1')),
                        help='Pausa em segundos entre cada envio (padrão: 1 ou variável READ_INTERVAL_SECONDS)')
    args = parser.parse_args(argv)
    if not args.start:
        parser.print_help()
        return 0
    if args.read_interval_seconds < 0:
        parser.error('Intervalo deve ser >= 0')
    if not args.bucket:
        parser.error('Defina S3_BUCKET ou --bucket')
    if not args.s3_key or args.s3_key.startswith('/'):
        parser.error('Defina S3_KEY ou --s3-key sem barra inicial')
    if not args.queue_url or not args.queue_url.endswith('.fifo'):
        parser.error('Defina SQS_QUEUE_URL ou --queue-url de uma fila .fifo')
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    s3, sqs = aws_clients()
    total = stream_dataset(s3, sqs, args.bucket, args.queue_url, args.s3_key, args.read_interval_seconds)
    LOG.info('Streaming concluído: %s registros enviados de s3://%s/%s', total, args.bucket, args.s3_key)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
