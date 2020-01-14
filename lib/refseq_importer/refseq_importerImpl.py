# -*- coding: utf-8 -*-
#BEGIN_HEADER
import logging
import os
import json
import requests

from installed_clients.GenomeFileUtilClient import GenomeFileUtil
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
    GIT_URL = ""
    GIT_COMMIT_HASH = ""

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
        with open(params['tsv_path']) as fd:
            for line in fd.readlines():
                cols = line.split("\t")
                url = cols[0]
                accession = cols[1]
                print(f'--- Checking accession {accession}')
                taxid = cols[2]
                source = "refseq " + cols[3]
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
                print(f"Fetching object {params['wsid']} from the workspace")
                resp = requests.post(ws_url, data=json.dumps(reqbody))
                assm = None
                if resp.ok:
                    info = resp.json()['result'][0]['data'][0]['info']
                    kbtype = info[2]
                    if kbtype == 'KBaseGenomes.Genome-17.0':
                        print(f'Already imported {accession}')
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
                    print(f'Error running genbank_to_genome for {accession}: {err}')
                    continue
                print(f'Done running genbank_to_genome for {accession}: {result}')
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
