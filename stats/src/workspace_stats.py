import os
import json
from kbase_workspace_client import WorkspaceClient

WS_TOK = os.environ['WS_TOK']
WS_ID = int(os.environ.get('WS_ID', 15792))
WS_URL = os.environ.get('WS_URL', 'https://ci.kbase.us/services/ws')
IS_ADMIN = bool(os.environ.get('WS_ADMIN'))


def main():
    counts = {}  # type: dict
    ws = WorkspaceClient(url=WS_URL, token=WS_TOK)
    for obj_info in ws.generate_obj_infos(WS_ID, admin=IS_ADMIN, latest=True):
        obj_type = obj_info[2]
        if obj_type not in counts:
            counts[obj_type] = 0
        counts[obj_type] += 1
    print('Total counts by type:')
    print(json.dumps(counts, indent=2))


if __name__ == '__main__':
    main()
