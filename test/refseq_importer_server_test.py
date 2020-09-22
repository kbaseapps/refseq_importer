# -*- coding: utf-8 -*-
import os
import unittest
from configparser import ConfigParser

from refseq_importer.refseq_importerImpl import refseq_importer
from refseq_importer.refseq_importerServer import MethodContext
from refseq_importer.authclient import KBaseAuth as _KBaseAuth
from refseq_importer.utils.init_db import init_db

from installed_clients.WorkspaceClient import Workspace


class RefseqImporterTest(unittest.TestCase):

    # Type hints for Mypy
    cfg = {}  # type: dict
    ctx = MethodContext(None)
    wsURL: str
    scratch: str
    wsClient: Workspace
    serviceImpl: refseq_importer
    callback_url: str
    wsName: str

    @classmethod
    def setUpClass(cls):
        token = os.environ.get('KB_AUTH_TOKEN', None)
        config_file = os.environ.get('KB_DEPLOYMENT_CONFIG', None)
        if not config_file:
            raise RuntimeError("Could not load config file")
        config = ConfigParser()
        config.read(config_file)
        for nameval in config.items('refseq_importer'):
            cls.cfg[nameval[0]] = nameval[1]
        # Getting username from Auth profile for token
        authServiceUrl = cls.cfg['auth-service-url']
        auth_client = _KBaseAuth(authServiceUrl)
        user_id = auth_client.get_user(token)
        # WARNING: don't call any logging methods on the context object,
        # it'll result in a NoneType error
        cls.ctx = MethodContext(None)
        cls.ctx.update({'token': token,
                        'user_id': user_id,
                        'provenance': [
                            {'service': 'refseq_importer',
                             'method': 'please_never_use_it_in_production',
                             'method_params': []
                             }],
                        'authenticated': 1})
        cls.wsURL = cls.cfg['workspace-url']
        cls.wsClient = Workspace(cls.wsURL)
        cls.serviceImpl = refseq_importer(cls.cfg)
        cls.scratch = cls.cfg['scratch']
        cls.callback_url = os.environ['SDK_CALLBACK_URL']

    def test_run_import(self):
        # Initialize the local state database
        # Does not overwrite previous entries
        print('Initializing the local state database...')
        init_db()
        # Run the imports
        self.serviceImpl.run_refseq_importer(self.ctx, {
            'wsname': "ReferenceDataManager",
            'wsid': 15792
        })
