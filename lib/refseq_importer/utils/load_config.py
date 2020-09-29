import yaml

_PATH = '/kb/module/importer_config.yaml'


def load_config():
    """
    importer_config.yaml must be in the current working directory
    """
    with open(_PATH) as fd:
        config = yaml.safe_load(fd)
    # Remove trailing slashes from URLs
    config['kbase_endpoint'] = config['kbase_endpoint'].strip('/')
    return config


config = load_config()
