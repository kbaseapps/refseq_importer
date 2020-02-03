"""
Mark an entry in the leveldb as status: done
"""
import json


def db_set_done(db, accession):
    key = accession.encode()
    if not db.get(key):
        raise RuntimeError(f"No such key in db: {key}")
    json_dict = json.loads(db.get(key).decode())
    json_dict['status'] = 'finished'
    db.put(key, json.dumps(json_dict).encode())
    print(f'Marked {accession} as finished')


def db_set_error(db, accession, msg):
    key = accession.encode()
    if not db.get(key):
        raise RuntimeError(f"No such key in db: {key}")
    json_dict = json.loads(db.get(key).decode())
    json_dict['status'] = 'error'
    json_dict['error'] = msg
    db.put(key, json.dumps(json_dict).encode())
    print(f'Marked {accession} as having an error')
