"""
Report stats from the LevelDB
"""
import json
import os
import plyvel

dbdir = os.environ.get('WORKDIR', '../test_local/workdir/import_state')
db = plyvel.DB(dbdir)

report_dir = os.environ.get('REPORT_DIR', './reports/')
errors_path = os.path.join(report_dir, 'errors.json')
overview_path = os.path.join(report_dir, 'overview.json')
finished_path = os.path.join(report_dir, 'finished.json')
pending_path = os.path.join(report_dir, 'pending.json')


def main():
    print(f"Loading database from {dbdir}")
    errors = dict()
    overview = {'total_errors': 0, 'total_finished': 0, 'total_pending': 0}
    finished = list()
    pending = list()
    print('Collecting database values...')
    for key, val in db:
        val = json.loads(val.decode())
        if val['status'] == 'error':
            overview['total_errors'] += 1
            err = val['error']
            errors[val['acc']] = err
        elif val['status'] == 'finished':
            overview['total_finished'] += 1
            finished.append(val['acc'])
        elif val['status'] == 'pending':
            overview['total_pending'] += 1
            pending.append(val['acc'])
        else:
            raise RuntimeError(f"Unknown status for entry {val}")
    print('...done')
    with open(errors_path, 'w') as fd:
        json.dump(errors, fd, indent=2)
        print(f'Wrote error report to {errors_path}')
    with open(overview_path, 'w') as fd:
        json.dump(overview, fd, indent=2)
        print(f'Wrote overview to {overview_path}')
    with open(finished_path, 'w') as fd:
        json.dump(finished, fd, indent=2)
        print(f'Wrote finished accessions list to {finished_path}')
    with open(pending_path, 'w') as fd:
        json.dump(pending, fd, indent=2)
        print(f'Wrote pending accessions list to {pending_path}')


def reset_errors():
    """
    Reset status entries for certain errors.
    """
    pass


if __name__ == '__main__':
    main()
