# -*- coding: utf-8 -*-
#BEGIN_HEADER
import sys
import plyvel
import logging
import os
import json
import requests
import shutil

from installed_clients.GenomeFileUtilClient import GenomeFileUtil
from refseq_importer.utils.db_update_entries import db_set_done, db_set_error
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
        gfu = GenomeFileUtil(self.callback_url)
        db_path = '/data/import_state'
        db = plyvel.DB(db_path)
        for (accession, json_bytes) in db:
            json_dict = json.loads(json_bytes.decode())
            if json_dict['status'] == 'finished':
                print(f'{accession} already completed')
                continue
            url = json_dict['url']
            accession = json_dict['acc']
            taxid = json_dict['tax']
            source = "refseq " + json_dict['src']
            print(f'Checking accession {accession}')
            # See if this accession already exists in KBase
            reqbody = {
                'method': 'get_objects2',
                'params': [{
                    'objects': [{
                        'wsid': params['wsid'],
                        'name': accession,
                    }],
                    'no_data': 1
                }]
            }
            endpoint = os.environ.get('KBASE_ENDPOINT', 'https://ci.kbase.us/services').strip('/')
            ws_url = endpoint + '/ws'
            resp = requests.post(ws_url, data=json.dumps(reqbody))
            assm = None
            if resp.ok:
                info = resp.json()['result'][0]['data'][0]['info']
                kbtype = info[2]
                if kbtype == 'KBaseGenomes.Genome-17.0':
                    print(f'Already imported {accession}')
                    db_set_done(db, accession)
                    continue
                metadata = info[-1]
                assm = metadata.get('Assembly Object')
            else:
                print('No existing genome object found')
            print(f'Running gfu.genbank_to_genome for {accession} with assembly {assm}')
            try:
                result = gfu.genbank_to_genome({
                    'file': {'ftp_url': url},
                    'source': source,
                    'taxon_id': str(taxid),
                    'genome_name': accession,
                    'workspace_name': params['workspace_name'],
                    'use_existing_assembly': assm
                })
            except Exception as err:
                msg = f'Error running genbank_to_genome for {accession}: {err}'
                sys.stderr.write(msg + '\n')
                db_set_error(db, accession, msg)
                print('Removing and recreating the temp directory..')
                shutil.rmtree(self.shared_folder)
                os.makedirs(self.shared_folder, exist_ok=True)
                print('Removed and recreated the temp directory')
                continue
            print(f'Done running genbank_to_genome for {accession}: {result}')
            db_set_done(db, accession)
            # Delete all data in the temp directory to keep filespace down.
            print('Removing and recreating the temp directory..')
            shutil.rmtree(self.shared_folder)
            os.makedirs(self.shared_folder, exist_ok=True)
            print('Removed and recreated the temp directory')
        # Clean up subjob files to keep disk space low
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
