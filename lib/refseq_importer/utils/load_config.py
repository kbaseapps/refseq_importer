import yaml

_PATH = '/kb/module/importer_config.yaml'


def load_config():
    """
    importer_config.yaml must be in the current working directory
    """
    with open(_PATH) as fd:
        return yaml.safe_load(fd)


config = load_config()
