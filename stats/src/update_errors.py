"""
Update error statuses to pending based on a pattern match for the error
message.
"""
import sys
import json
import os
import plyvel

dbdir = os.environ.get('WORKDIR', '../test_local/workdir/import_state')
db = plyvel.DB(dbdir)


def main(pattern: str):
    print(f"Loading database from {dbdir}")
    total_count = 0
    for key, val in db:
        val = json.loads(val.decode())
        if val.get('status') != 'error' or pattern not in val['error']:
            continue
        val['status'] = 'pending'
        del val['error']
        db.put(key, json.dumps(val).encode())
        print(f"Reset entry {key} to 'pending' status")
        total_count += 1
    print(f'Done resetting {total_count} entries.')


if __name__ == '__main__':
    if len(sys.argv) <= 1:
        sys.stderr.write("Pass an error message string pattern\n")
        sys.exit(1)
    print(f"Matching errors with pattern '{sys.argv[1]}'")
    main(sys.argv[1])
