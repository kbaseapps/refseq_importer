# -*- coding: utf-8 -*-
#BEGIN_HEADER
import plyvel
import logging
import os
import time
import json

from refseq_importer.utils.run_import import run_import, run_batch_import
from refseq_importer.utils.load_config import config
#END_HEADER


class refseq_importer:
    '''
    Module Name:
    refseq_importer

    Module Description:
    A KBase module: refseq_importer
    '''

    ######## WARNING FOR GEVENT USERS ####### noqa
    # Since asynchronous IO can lead to methods - even the same method -
    # interrupting each other, you must be *very* careful when using global
    # state. A method could easily clobber the state set by another while
    # the latter method is running.
    ######################################### noqa
    VERSION = "0.0.1"
    GIT_URL = "https://github.com/kbaseapps/refseq_importer"
    GIT_COMMIT_HASH = "17d6c3762c7a6a42f82cea08007341f9da090754"

    #BEGIN_CLASS_HEADER
    #END_CLASS_HEADER

    # config contains contents of config file in a hash or None if it couldn't
    # be found
    def __init__(self, config):
        #BEGIN_CONSTRUCTOR
        self.callback_url = os.environ['SDK_CALLBACK_URL']
        self.scratch = config['scratch']
        logging.basicConfig(format='%(created)s %(levelname)s: %(message)s',
                            level=logging.INFO)
        #END_CONSTRUCTOR
        pass

    def run_refseq_importer(self, ctx, params):
        """
        :param params: instance of mapping from String to unspecified object
        :returns: instance of type "ReportResults" -> structure: parameter
           "report_name" of String, parameter "report_ref" of String
        """
        # ctx is the context object
        # return variables are: output
        #BEGIN run_refseq_importer
        db_path = '/kb/module/work/import_state'
        db = plyvel.DB(db_path)
        jobs = []
        for (accession, json_bytes) in db:
            accession = accession.decode()
            json_dict = json.loads(json_bytes.decode())
            if json_dict['status'] == 'finished':
                print(f'{accession} already completed')
                continue
            if json_dict['status'] == 'error':
                print(f'{accession} had an error before')
                continue
            else:
                params = {
                    'wsid': params['wsid'],
                    'wsname': params['wsname'],
                    'import_data': json_dict
                }
                jobs.append((accession, params))
                if len(jobs) == config['batch_size']:
                    print('*' * 80)
                    print(f'Running {len(jobs)} jobs..')
                    start = time.time()
                    run_batch_import(jobs, self, ctx, db)
                    print(f'..Done running {len(jobs)} jobs in {time.time() - start}s.')
                    jobs = []
                    print('*' * 80)
        output = {}  # type: dict
        #END run_refseq_importer

        # At some point might do deeper type checking...
        if not isinstance(output, dict):
            raise ValueError('Method run_refseq_importer return value ' +
                             'output is not type dict as required.')
        # return the results
        return [output]

    def run_single_import(self, ctx, params):
        """
        :param params: instance of mapping from String to unspecified object
        :returns: instance of type "ReportResults" -> structure: parameter
           "report_name" of String, parameter "report_ref" of String
        """
        # ctx is the context object
        # return variables are: output
        #BEGIN run_single_import
        # Validation is minimal. This module is for internal developer use.
        for key in ['wsid', 'wsname', 'import_data']:
            if key not in params:
                raise RuntimeError(f'{key} required in params')
        wsid = params['wsid']
        wsname = params['wsname']
        import_data = params['import_data']
        try:
            run_import(self.callback_url, self.scratch, wsid, wsname,
                       import_data)
            output = {'accession': import_data['acc']}
        except Exception as err:
            output = {'error': str(err)}
        #END run_single_import

        # At some point might do deeper type checking...
        if not isinstance(output, dict):
            raise ValueError('Method run_single_import return value ' +
                             'output is not type dict as required.')
        # return the results
        return [output]

    def status(self, ctx):
        #BEGIN_STATUS
        returnVal = {
            'state': "OK",
            'message': "",
            'version': self.VERSION,
            'git_url': self.GIT_URL,
            'git_commit_hash': self.GIT_COMMIT_HASH
        }
        #END_STATUS
        return [returnVal]
