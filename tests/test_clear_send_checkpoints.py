import importlib.util
import sqlite3

import pytest

spec = importlib.util.spec_from_file_location('clear_send_checkpoints', 'scripts/clear_send_checkpoints.py')
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


def test_only_selected_queue_checkpoints_are_deleted(tmp_path):
    path = tmp_path / 'state.sqlite3'
    with sqlite3.connect(path) as db:
        db.execute('CREATE TABLE sent (destination TEXT, id TEXT)')
        db.execute('CREATE TABLE records (id TEXT)')
        db.execute('CREATE TABLE files (name TEXT)')
        db.executemany('INSERT INTO sent VALUES (?, ?)', [('queue-a', '1'), ('queue-a', '2'), ('queue-b', '3')])
        db.execute("INSERT INTO records VALUES ('record-1')")
        db.execute("INSERT INTO files VALUES ('input.csv')")
    assert module.clear_checkpoints(path, 'queue-a') == 2
    with sqlite3.connect(path) as db:
        assert db.execute('SELECT * FROM sent').fetchall() == [('queue-b', '3')]
        assert db.execute('SELECT * FROM records').fetchall() == [('record-1',)]
        assert db.execute('SELECT * FROM files').fetchall() == [('input.csv',)]
    assert module.clear_checkpoints(path, 'queue-a') == 0


def test_missing_state_is_not_created(tmp_path):
    path = tmp_path / 'missing.sqlite3'
    with pytest.raises(ValueError, match='not found'):
        module.clear_checkpoints(path, 'queue')
    assert not path.exists()


def test_running_producer_blocks_reset(tmp_path):
    import fcntl
    path = tmp_path / 'state.sqlite3'
    path.touch()
    with (tmp_path / '.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        with pytest.raises(RuntimeError, match='Producer is running'):
            module.clear_checkpoints(path, 'queue')


def test_full_reset_removes_downloads_database_and_consolidated_files(tmp_path):
    directory = tmp_path / 'data'
    directory.mkdir()
    (directory / 'raw').mkdir()
    (directory / 'raw' / 'input.csv').write_text('downloaded')
    for name in ['consolidated.csv', 'consolidated.tmp', 'state.sqlite3', 'state.sqlite3-wal',
                 'state.sqlite3-shm', 'state.sqlite3-journal', 'state.sqlite3.before-reset.bak']:
        (directory / name).write_text('data')
    (directory / 'notes.txt').write_text('keep')
    removed = module.reset_local_data(directory / 'state.sqlite3')
    assert len(removed) == 8
    assert sorted(p.name for p in directory.iterdir()) == ['.lock', 'notes.txt']
    assert module.reset_local_data(directory / 'state.sqlite3') == []


def test_full_reset_refuses_running_producer(tmp_path):
    import fcntl
    (tmp_path / 'state.sqlite3').write_text('keep')
    with (tmp_path / '.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        with pytest.raises(RuntimeError, match='Producer is running'):
            module.reset_local_data(tmp_path / 'state.sqlite3')
    assert (tmp_path / 'state.sqlite3').read_text() == 'keep'


def test_full_reset_does_not_follow_raw_symlink(tmp_path):
    data = tmp_path / 'data'
    external = tmp_path / 'external'
    data.mkdir()
    external.mkdir()
    (external / 'keep.csv').write_text('keep')
    (data / 'raw').symlink_to(external, target_is_directory=True)
    module.reset_local_data(data / 'state.sqlite3')
    assert (external / 'keep.csv').read_text() == 'keep'
    assert not (data / 'raw').is_symlink()


def test_full_reset_missing_directory_is_noop(tmp_path):
    assert module.reset_local_data(tmp_path / 'missing' / 'state.sqlite3') == []
    assert not (tmp_path / 'missing').exists()
