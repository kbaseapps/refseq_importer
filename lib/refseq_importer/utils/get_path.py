

def get_path(data, path):
    """
    Fetch a value in a nested dict/list using a path of keys/indexes
    If it fails at any point in the path, None is returned
    example: get_path({'x': [1, {'y': 'result'}]}, ['x', 1, 'y'])
    """
    current = data
    for p in path:
        try:
            current = data[p]
        except Exception:
            return None
    return current
