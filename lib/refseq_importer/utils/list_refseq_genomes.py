"""
Writes a TSV file of the genbank URL and taxonomy ID for every Refseq genome in
these categories: bacteria, archaea, plant, and fungi
"""
from ftplib import FTP

_CATEGORIES = ["archaea", "bacteria", "plant", "fungi"]
_HOST = "ftp.ncbi.nlm.nih.gov"
_BASE_PATH = "/genomes/refseq"
_SUMMARY_FILENAME = "assembly_summary.txt"
_OUT_PATH = "refseq_full.tsv"


def retr_callback(line):
    """Called with every line of the assembly_summary.txt from the Refseq ftp server."""
    if line.strip()[0] == '#':
        # Skip comment lines
        return
    cols = line.split("\t")
    ftp_url = cols[19]
    _id = cols[0]
    tax_id = cols[5]
    dirname = ftp_url.split('/')[-1]
    full_url = f"{ftp_url}/{dirname}_genomic.gbff.gz"
    line = f"{full_url}\t{_id}\t{tax_id}\n"
    # Preferably we only open the file once and keep appending, but not sure
    # how to do that in this callback
    with open(_OUT_PATH, "a") as fd:
        fd.write(line)


def list_refseq_genomes():
    """
    Crawl the NCBI FTP servers and fetch the refseq info we need.
    """
    ftp = FTP(_HOST)
    ftp.login()
    # Clear out any existing output file
    with open(_OUT_PATH, "w") as fd:
        fd.write("")
    for cat in _CATEGORIES:
        ftp.cwd(f"{_BASE_PATH}/{cat}")
        ftp.retrlines(f'RETR {_SUMMARY_FILENAME}', retr_callback)


if __name__ == '__main__':
    list_refseq_genomes()
