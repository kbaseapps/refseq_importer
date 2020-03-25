import time
import requests
import os
import json
from installed_clients.GenomeFileUtilClient import GenomeFileUtil


def run_import(callback_url, scratch, params, accession, json_dict):
    """
    Run the actual import operation.
    Pass in a dict with keys:
        'url' - source genome FTP url
        'acc' - Refseq accession
        'tax' - NCBI taxonomy IDJk
        'src' - Refseq category such as "reference genome", "representative genome", "na"
    """
    print(f'Running import for accession {accession}')
    gfu = GenomeFileUtil(callback_url)
    start = time.time()
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
            return
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
        raise RuntimeError(f'Error running genbank_to_genome for {accession}: {err}')
    print(f'GFU genbank_to_genome result is {result}')
    print(f'Import of {accession} completed. Total import time was {time.time() - start}')
    return accession
