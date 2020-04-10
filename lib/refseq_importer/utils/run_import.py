import time
import requests
import os
import json
from installed_clients.GenomeFileUtilClient import GenomeFileUtil


def run_import(callback_url, scratch, wsid, wsname, import_data):
    """
    Run the actual import operation.
    `import_data` is a dict with keys:
        'url' - source genome FTP url
        'acc' - Refseq accession
        'tax' - NCBI taxonomy IDJk
        'src' - Refseq category such as "reference genome", "representative genome", "na"
    """
    print(f'Running import for accession {import_data["acc"]}')
    gfu = GenomeFileUtil(callback_url)
    start = time.time()
    url = import_data['url']
    accession = import_data['acc']
    taxid = import_data['tax']
    source = "refseq " + import_data['src']
    print(f'Checking accession {accession}')
    # See if this accession already exists in KBase
    reqbody = {
        'method': 'get_objects2',
        'params': [{
            'objects': [{
                'wsid': wsid,
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
            'workspace_name': wsname,
            'use_existing_assembly': assm
        })
    except Exception as err:
        raise RuntimeError(f'Error running genbank_to_genome for {accession}: {err}')
    print(f'GFU genbank_to_genome result is {result}')
    print(f'Import of {accession} completed. Total import time was {time.time() - start}')
    return accession
