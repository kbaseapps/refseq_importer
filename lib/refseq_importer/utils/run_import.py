import time
import requests
import json
from typing import Dict
from concurrent.futures import ThreadPoolExecutor, as_completed, Future

from refseq_importer.utils.db_update_entries import db_set_error, db_set_done
from installed_clients.GenomeFileUtilClient import GenomeFileUtil
from refseq_importer.utils.load_config import config


def run_batch_import(jobs: Dict[Future, str], impl, ctx, db):
    """
    Run a batch of import jobs using threading and process the results
    """
    # Run the threads
    with ThreadPoolExecutor(max_workers=config['batch_size']) as executor:
        # Dictionary of {future: accession}
        # Following this example:
        # https://docs.python.org/3/library/concurrent.futures.html#threadpoolexecutor-example
        futures = dict()
        # Start all the threads
        for (accession, params) in jobs:
            future = executor.submit(impl.run_single_import, ctx, params)
            futures[future] = accession
        # Process all the results
        for future in as_completed(futures):
            accession = futures[future]
            try:
                data = future.result()
            except Exception as exc:
                print(f"{accession} had an error")
                db_set_error(db, accession, str(exc))
                continue
            result = data[0]
            if 'error' in result:
                print(f"{accession} had an error")
                db_set_error(db, accession, result['error'])
            elif 'accession' in result:
                print(f"{accession} successfully imported")
                db_set_done(db, accession)


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
    ws_url = config['kbase_endpoint'] + '/ws'
    resp = requests.post(ws_url, data=json.dumps(reqbody))
    assm = None
    if resp.ok:
        info = resp.json()['result'][0]['data'][0]['info']
        kbtype = info[2]
        print('Existing object found with type: ', kbtype)
        if kbtype == config['genome_type_version']:
            print(f'Already imported {accession}')
            return
        print('No match on type, using previous assembly')
        metadata = info[-1]
        assm = metadata.get('Assembly Object')
    else:
        print('No existing genome object found. Response was: ', resp.text)
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
