"""Local producer cleanup helpers. No AWS calls."""
from pathlib import Path
import shutil
import sqlite3


def clear_checkpoints(path, queue_url):
    path = Path(path).resolve()
    if not path.is_file():
        raise ValueError(f'Producer state not found: {path}')
    import fcntl
    with (path.parent / '.lock').open('a') as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise RuntimeError('Producer is running. Stop it before clearing checkpoints.') from None
        with sqlite3.connect(path.as_uri() + '?mode=rw', uri=True) as db:
            result = db.execute('DELETE FROM sent WHERE destination = ?', (queue_url,))
            return result.rowcount



def reset_local_data(state_file):
    """Remove managed producer artifacts while holding its lock; retain .lock."""
    path = Path(state_file).absolute()
    if path.name in ('.lock', 'raw', 'consolidated.csv', 'consolidated.tmp'):
        raise ValueError('Invalid producer state filename')
    directory = path.parent.resolve()
    if not directory.exists():
        return []
    if directory in (Path('/'), Path.home(), Path(__file__).resolve().parents[1]):
        raise ValueError('Use the producer data directory, not root, home or the project root')
    lock_path = directory / '.lock'
    if lock_path.is_symlink():
        raise ValueError('Producer lock must not be a symbolic link')
    import fcntl
    with lock_path.open('a') as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise RuntimeError('Producer is running. Stop it before resetting local data.') from None
        targets = [directory / 'raw', directory / 'consolidated.csv', directory / 'consolidated.tmp', directory / 's3-dataset.json', directory / 's3-dataset.tmp']
        targets += [directory / (path.name + suffix) for suffix in
                    ('', '-wal', '-shm', '-journal', '.before-reset.bak',
                     '.before-reset.bak-wal', '.before-reset.bak-shm', '.before-reset.bak-journal')]
        removed = []
        for target in targets:
            if target.is_symlink():
                target.unlink()  # Never follow links into another directory.
            elif target.is_dir():
                if target.name != 'raw':
                    raise ValueError(f'Expected a file, found directory: {target}')
                shutil.rmtree(target)
            elif target.exists():
                target.unlink()
            else:
                continue
            removed.append(str(target))
        return removed
