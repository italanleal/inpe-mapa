#!/usr/bin/env python3
"""Clear send checkpoints, or use --full-reset to erase all local producer data. No AWS calls."""
import argparse
import os
from pathlib import Path
import sqlite3
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from producer.local_state import clear_checkpoints, reset_local_data

DEFAULT_QUEUE = 'https://sqs.sa-east-1.amazonaws.com/053109836747/inpe-focos.fifo'



def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--state-file', type=Path,
                        default=Path(os.getenv('DATA_DIR', 'data')) / 'state.sqlite3')
    parser.add_argument('--queue-url', default=os.getenv('SQS_QUEUE_URL', DEFAULT_QUEUE))
    parser.add_argument('--full-reset', action='store_true',
                        help='Delete downloaded CSVs, consolidated dataset, SQLite records/checkpoints and reset backups locally')
    args = parser.parse_args(argv)
    try:
        if args.full_reset:
            removed = reset_local_data(args.state_file)
            print(f'Full local reset complete: removed {len(removed)} managed artifacts.')
            for path in removed:
                print(f'  {path}')
            print('AWS data and credentials were not changed. The producer will download and consolidate again.')
            return 0
        deleted = clear_checkpoints(args.state_file, args.queue_url)
    except (ValueError, RuntimeError, OSError, sqlite3.Error) as error:
        print(f'Error: {error}', file=sys.stderr)
        return 1
    print(f'Cleared {deleted} send checkpoints for {args.queue_url}.')
    print('Downloaded files, consolidated records and AWS data were preserved.')
    return 0


if __name__ == '__main__':
    sys.exit(main())
