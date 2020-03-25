# -*- coding: utf-8 -*-
#BEGIN_HEADER
import plyvel
import logging
import os
import json
import shutil
import concurrent.futures

MAX_WORKERS = int(os.environ.get('MAX_THREAD_WORKERS', 5))

from installed_clients.GenomeFileUtilClient import GenomeFileUtil
from refseq_importer.utils.run_import import run_import
from refseq_importer.utils.db_update_entries import db_set_error, db_set_done
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
    GIT_COMMIT_HASH = "82d0dcc15d0063b3f2558e49cdf31f79876d105b"

    #BEGIN_CLASS_HEADER
    #END_CLASS_HEADER

    # config contains contents of config file in a hash or None if it couldn't
    # be found
    def __init__(self, config):
        #BEGIN_CONSTRUCTOR
        self.callback_url = os.environ['SDK_CALLBACK_URL']
        self.shared_folder = config['scratch']
        logging.basicConfig(format='%(created)s %(levelname)s: %(message)s',
                            level=logging.INFO)
        #END_CONSTRUCTOR
        pass


    def run_refseq_importer(self, ctx, params):
        """
        This example function accepts any number of parameters and returns results in a KBaseReport
        :param params: instance of mapping from String to unspecified object
        :returns: instance of type "ReportResults" -> structure: parameter
           "report_name" of String, parameter "report_ref" of String
        """
        # ctx is the context object
        # return variables are: output
        #BEGIN run_refseq_importer
        db_path = '/kb/module/work/import_state'
        db = plyvel.DB(db_path)
        batch = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            for (accession, json_bytes) in db:
                accession = accession.decode()
                json_dict = json.loads(json_bytes.decode())
                if json_dict['status'] == 'finished':
                    print(f'{accession} already completed')
                    continue
                if json_dict['status'] == 'error':
                    print(f'{accession} had an error before: {json_bytes}')
                    continue
                else:
                    batch.append((accession, json_dict))
                if len(batch) >= 4:
                    print(f'Submitting import jobs for: {batch}"')
                    # List of pairs of (accession, future)
                    futures = [
                        (
                            accession,
                            executor.submit(
                                run_import,
                                self.callback_url,
                                self.shared_folder,
                                params,
                                accession,
                                json_dict
                            )
                        )
                        for (accession, json_dict)
                        in batch
                    ]
                    for (accession, future) in futures:
                        try:
                            future.result(timeout=600)
                            db_set_done(db, accession)
                            print(f"{accession} successfully completed")
                        except concurrent.futures.TimeoutError:
                            db_set_error(db, accession, "Timed out")
                            print(f"{accession} timed out and failed to import.")
                        except Exception as err:
                            db_set_error(db, accession, str(err))
                            print(f"{accession} failed to import with error: {err}.")
                    batch = []
                    # Keep disk usage low
                    print('Removing and recreating the temp directory..')
                    shutil.rmtree(self.shared_folder)
                    os.makedirs(self.shared_folder, exist_ok=True)
        output = {}  # type: dict
        #END run_refseq_importer

        # At some point might do deeper type checking...
        if not isinstance(output, dict):
            raise ValueError('Method run_refseq_importer return value ' +
                             'output is not type dict as required.')
        # return the results
        return [output]
    def status(self, ctx):
        #BEGIN_STATUS
        returnVal = {'state': "OK",
                     'message': "",
                     'version': self.VERSION,
                     'git_url': self.GIT_URL,
                     'git_commit_hash': self.GIT_COMMIT_HASH}
        #END_STATUS
        return [returnVal]
