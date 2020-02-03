import json
import plyvel
from refseq_importer.utils.list_refseq_genomes import list_refseq_genomes

_DB_PATH = '/data/import_state'
_TSV_PATH = '/data/refseq_full.tsv'


def init_db():
    list_refseq_genomes()
    db = plyvel.DB(_DB_PATH, create_if_missing=True)
    with open(_TSV_PATH) as fd:
        for line in fd.readlines():
            [url, accession, taxid, source] = line.split("\t")
            key = accession.encode()
            if not db.get(key):
                json_bytes = json.dumps({
                    'status': 'pending',
                    'url': url,
                    'acc': accession,
                    'tax': taxid,
                    'src': source
                }).encode()
                db.put(key, json_bytes)
                print(accession, 'saved to the db')
            else:
                print(accession, 'already exists')


if __name__ == '__main__':
    init_db()
