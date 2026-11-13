#!/usr/bin/env python3
"""Empty the project's DynamoDB table and S3 bucket; keep the resources.

Preview by default. --execute permanently deletes all table items and all S3
object versions/delete markers. Stop the producer and drain SQS beforehand.
Uses boto3 (already in producer/requirements.txt); no credentials in this file.
"""
import argparse
import json
import logging
import os
from pathlib import Path
import sqlite3
import sys
import tempfile

LOG = logging.getLogger('reset')
DEFAULT_ACCOUNT = '053109836747'
DEFAULT_TABLE = 'inpe-focos-clusters'
DEFAULT_BUCKET = 'inpe-focos-italan-2026-bucket'


def count_table(table):
    count = 0
    request = {'Select': 'COUNT', 'ConsistentRead': True}
    while True:
        page = table.scan(**request)
        count += page.get('Count', 0)
        last = page.get('LastEvaluatedKey')
        if not last:
            return count
        request['ExclusiveStartKey'] = last


def empty_table(table):
    # Complete the scan before deleting so pagination is independent of deletion.
    # Temporary file bounds memory even for larger tables.
    request = {'ProjectionExpression': '#pk, #sk',
               'ExpressionAttributeNames': {'#pk': 'pk', '#sk': 'sk'}, 'ConsistentRead': True}
    count = 0
    with tempfile.TemporaryFile(mode='w+t', encoding='utf-8') as keys:
        while True:
            page = table.scan(**request)
            for item in page.get('Items', []):
                keys.write(json.dumps({'pk': item['pk'], 'sk': item['sk']}) + '\n')
            last = page.get('LastEvaluatedKey')
            if not last:
                break
            request['ExclusiveStartKey'] = last
        keys.seek(0)
        # boto3's batch_writer retries unprocessed DynamoDB requests.
        with table.batch_writer() as batch:
            for line in keys:
                batch.delete_item(Key=json.loads(line))
                count += 1
    if table.scan(Select='COUNT', ConsistentRead=True, Limit=1).get('Count', 0):
        raise RuntimeError('DynamoDB still contains items. Stop all writers before retrying.')
    return count


def count_s3(s3, bucket, account):
    versions = markers = 0
    for page in s3.get_paginator('list_object_versions').paginate(Bucket=bucket, ExpectedBucketOwner=account):
        versions += len(page.get('Versions', []))
        markers += len(page.get('DeleteMarkers', []))
    return versions, markers


def empty_s3(s3, bucket, account):
    versions = markers = 0
    while True:
        # Re-read the first page after each batch: avoids retaining pagination
        # markers for versions we have just removed. Also handles null versions.
        page = s3.list_object_versions(Bucket=bucket, ExpectedBucketOwner=account, MaxKeys=1000)
        items = page.get('Versions', []) + page.get('DeleteMarkers', [])
        if not items:
            break
        result = s3.delete_objects(
            Bucket=bucket, ExpectedBucketOwner=account,
            Delete={'Objects': [{'Key': item['Key'], 'VersionId': item['VersionId']} for item in items], 'Quiet': True})
        if result.get('Errors'):
            errors = result['Errors']
            raise RuntimeError(f'S3 deletion partly failed: {errors[:3]}. Cleanup is incomplete; correct the error and rerun.')
        versions += len(page.get('Versions', []))
        markers += len(page.get('DeleteMarkers', []))
        LOG.info('S3: deleted %s versions and %s delete markers', versions, markers)
    remaining = s3.list_objects_v2(Bucket=bucket, ExpectedBucketOwner=account, MaxKeys=1)
    if remaining.get('Contents'):
        raise RuntimeError('S3 still contains objects. Stop all writers before retrying.')
    return versions, markers


def clear_checkpoints(path, queue_url):
    """Optional local replay setup; does not erase downloaded files/observations."""
    if not path.is_file():
        raise ValueError(f'Producer state not found: {path}')
    import fcntl
    with (path.parent / '.lock').open('a') as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise RuntimeError('Producer is running; stop it before clearing checkpoints.') from None
        with sqlite3.connect(f'file:{path.resolve()}?mode=rw', uri=True) as db:
            # Preserve a restorable local backup before changing send state.
            backup = path.with_name(path.name + '.before-reset.bak')
            if backup.exists():
                raise ValueError(f'Checkpoint backup already exists: {backup}. Move it before another reset.')
            with sqlite3.connect(backup) as destination:
                db.backup(destination)
            result = db.execute('DELETE FROM sent WHERE destination=?', (queue_url,))
            return result.rowcount


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--profile', help='AWS profile with cleanup permissions; omit in CloudShell')
    parser.add_argument('--region', default='sa-east-1')
    parser.add_argument('--account-id', default=DEFAULT_ACCOUNT)
    parser.add_argument('--table', default=DEFAULT_TABLE)
    parser.add_argument('--bucket', default=DEFAULT_BUCKET)
    parser.add_argument('--execute', action='store_true', help='Permanently delete the data; default is preview only')
    parser.add_argument('--reset-checkpoints', action='store_true', help='Also clear local send checkpoints after successful AWS cleanup')
    parser.add_argument('--state-file', type=Path, default=Path(os.getenv('DATA_DIR', 'data')) / 'state.sqlite3')
    parser.add_argument('--queue-url', default=f'https://sqs.sa-east-1.amazonaws.com/{DEFAULT_ACCOUNT}/inpe-focos.fifo')
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format='%(levelname)s %(message)s')
    try:
        import boto3
        from botocore.config import Config
        session = boto3.Session(profile_name=args.profile, region_name=args.region)
        config = Config(retries={'mode': 'standard', 'max_attempts': 8}, connect_timeout=10, read_timeout=60)
        identity = session.client('sts', config=config).get_caller_identity()
        if identity['Account'] != args.account_id:
            raise ValueError(f"Wrong AWS account: {identity['Account']}; expected {args.account_id}")
        table = session.resource('dynamodb', config=config).Table(args.table)
        table.load()
        if table.table_arn.split(':')[4] != args.account_id:
            raise ValueError('Table belongs to another account')
        schema = {(k['AttributeName'], k['KeyType']) for k in table.key_schema}
        if schema != {('pk', 'HASH'), ('sk', 'RANGE')}:
            raise ValueError('Expected table keys pk/sk; refusing to delete another schema')
        s3 = session.client('s3', config=config)
        # Read-only preview of BOTH targets before any deletion.
        count = count_table(table)
        versions, markers = count_s3(s3, args.bucket, args.account_id)
        LOG.info('Account %s | region %s | identity %s', args.account_id, args.region, identity['Arn'])
        LOG.info('Table %s: %s items | bucket %s: %s versions, %s delete markers', args.table, count, args.bucket, versions, markers)
        if args.reset_checkpoints:
            if not args.state_file.is_file():
                raise ValueError(f'Producer state not found: {args.state_file}')
            if args.state_file.with_name(args.state_file.name + '.before-reset.bak').exists():
                raise ValueError('Checkpoint backup already exists; move it before resetting again')
            LOG.info('Local send checkpoints for %s will also be cleared', args.queue_url)
        if not args.execute:
            LOG.info('DRY RUN: nothing deleted. Add --execute to permanently delete the data.')
            return 0
        LOG.warning('Deleting data. The producer must be stopped and SQS drained. Table, index, and bucket will remain.')
        deleted = empty_table(table)
        LOG.info('DynamoDB: deleted %s items', deleted)
        versions, markers = empty_s3(s3, args.bucket, args.account_id)
        LOG.info('S3: deleted %s versions and %s delete markers', versions, markers)
        if args.reset_checkpoints:
            cleared = clear_checkpoints(args.state_file, args.queue_url)
            LOG.info('Cleared %s local send checkpoints (SQLite backup saved)', cleared)
        LOG.info('Cleanup complete. Allow time for the DynamoDB index to reflect deletions.')
        return 0
    except Exception as error:
        LOG.error('%s', error)
        LOG.error('Cleanup was not completed. Some earlier deletions may have succeeded; rerun after fixing the error.')
        return 1


if __name__ == '__main__':
    sys.exit(main())
