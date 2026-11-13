import importlib.util
import sqlite3
import sys
from types import ModuleType
from unittest.mock import Mock, MagicMock

import pytest

spec = importlib.util.spec_from_file_location('reset_aws_data', 'scripts/reset_aws_data.py')
reset = importlib.util.module_from_spec(spec)
spec.loader.exec_module(reset)


def test_dynamodb_all_pages_deleted_and_verified():
    table = MagicMock()
    table.scan.side_effect = [
        {'Items': [{'pk': 'DAY#x', 'sk': 'cluster'}], 'LastEvaluatedKey': {'pk': 'DAY#x', 'sk': 'cluster'}},
        {'Items': [{'pk': 'EVENT#x', 'sk': 'marker'}]}, {'Count': 0}]
    assert reset.empty_table(table) == 2
    writer = table.batch_writer.return_value.__enter__.return_value
    assert writer.delete_item.call_count == 2
    assert table.scan.call_args_list[1].kwargs['ExclusiveStartKey'] == {'pk': 'DAY#x', 'sk': 'cluster'}
    assert table.scan.call_args.kwargs['ConsistentRead'] is True


def test_s3_versions_markers_and_null_version():
    s3 = Mock()
    s3.list_object_versions.side_effect = [
        {'Versions': [{'Key': 'data.csv', 'VersionId': 'v1'}], 'DeleteMarkers': [{'Key': 'data.csv', 'VersionId': 'd1'}]},
        {'Versions': [{'Key': 'old.csv', 'VersionId': 'null'}]}, {}]
    s3.delete_objects.return_value = {}
    s3.list_objects_v2.return_value = {}
    assert reset.empty_s3(s3, 'bucket', 'account') == (2, 1)
    assert s3.delete_objects.call_count == 2
    assert s3.delete_objects.call_args_list[0].kwargs['Delete']['Objects'] == [
        {'Key': 'data.csv', 'VersionId': 'v1'}, {'Key': 'data.csv', 'VersionId': 'd1'}]
    assert s3.delete_objects.call_args.kwargs['ExpectedBucketOwner'] == 'account'
    assert s3.delete_objects.call_args.kwargs['Delete']['Objects'][0]['VersionId'] == 'null'


def test_s3_partial_failures_are_not_reported_as_success():
    s3 = Mock()
    s3.list_object_versions.return_value = {'Versions': [{'Key': 'data', 'VersionId': 'v1'}]}
    s3.delete_objects.return_value = {'Errors': [{'Key': 'data', 'Code': 'AccessDenied'}]}
    with pytest.raises(RuntimeError, match='partly failed'):
        reset.empty_s3(s3, 'bucket', 'account')
    assert s3.list_object_versions.call_count == 1


def test_preview_counts_all_pages():
    table = Mock()
    table.scan.side_effect = [{'Count': 3, 'LastEvaluatedKey': {'pk': 'a', 'sk': 'b'}}, {'Count': 4}]
    assert reset.count_table(table) == 7
    table.batch_writer.assert_not_called()
    s3 = Mock()
    s3.get_paginator.return_value.paginate.return_value = [
        {'Versions': [{'Key': 'a'}]}, {'DeleteMarkers': [{'Key': 'b'}]}]
    assert reset.count_s3(s3, 'bucket', 'account') == (1, 1)
    s3.delete_objects.assert_not_called()


def test_checkpoint_reset_keeps_other_destinations_and_saves_backup(tmp_path):
    path = tmp_path / 'state.sqlite3'
    with sqlite3.connect(path) as db:
        db.execute('CREATE TABLE sent (destination TEXT, id TEXT)')
        db.executemany('INSERT INTO sent VALUES (?, ?)', [('target', 'a'), ('other', 'b')])
    assert reset.clear_checkpoints(path, 'target') == 1
    with sqlite3.connect(path) as db:
        assert db.execute('SELECT * FROM sent').fetchall() == [('other', 'b')]
    with sqlite3.connect(path.with_name('state.sqlite3.before-reset.bak')) as backup:
        assert backup.execute('SELECT COUNT(*) FROM sent').fetchone()[0] == 2
    with pytest.raises(ValueError, match='backup already exists'):
        reset.clear_checkpoints(path, 'target')


def test_dry_run_never_calls_delete_and_account_mismatch_stops(monkeypatch):
    sdk = ModuleType('boto3')
    session = MagicMock()
    sdk.Session = Mock(return_value=session)
    session.client.return_value.get_caller_identity.return_value = {'Account': reset.DEFAULT_ACCOUNT, 'Arn': 'test-identity'}
    table = session.resource.return_value.Table.return_value
    table.table_arn = f'arn:aws:dynamodb:sa-east-1:{reset.DEFAULT_ACCOUNT}:table/inpe-focos-clusters'
    table.key_schema = [{'AttributeName': 'pk', 'KeyType': 'HASH'}, {'AttributeName': 'sk', 'KeyType': 'RANGE'}]
    config = ModuleType('botocore.config')
    config.Config = Mock()
    monkeypatch.setitem(sys.modules, 'boto3', sdk)
    monkeypatch.setitem(sys.modules, 'botocore.config', config)
    monkeypatch.setattr(reset, 'count_table', Mock(return_value=2))
    monkeypatch.setattr(reset, 'count_s3', Mock(return_value=(3, 1)))
    delete_table = Mock()
    delete_s3 = Mock()
    monkeypatch.setattr(reset, 'empty_table', delete_table)
    monkeypatch.setattr(reset, 'empty_s3', delete_s3)
    assert reset.main([]) == 0
    delete_table.assert_not_called()
    delete_s3.assert_not_called()
    assert reset.main(['--account-id', 'wrong', '--execute']) == 1
    delete_table.assert_not_called()
    delete_s3.assert_not_called()
