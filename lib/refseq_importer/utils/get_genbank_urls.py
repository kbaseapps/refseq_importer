from ftplib import FTP

_HOST = "ftp.ncbi.nlm.nih.gov"

_IN_PATH = "refseq.tsv"
_OUT_PATH = "refseq_full_urls.tsv"


def main():
    ftp = FTP(_HOST)
    ftp.login()
    with open(_OUT_PATH, 'w') as fd:
        fd.write("")
    with open(_IN_PATH) as fd_inp, open(_OUT_PATH, 'a') as fd_out:
        for line in fd_inp.readlines():
            cols = line.split("\t")
            base_url = cols[0]
            base_path = base_url.replace(f"ftp://{_HOST}", "")
            ftp.cwd(base_path)
            gb_name = None
            for entry in ftp.mlsd():
                filename = entry[0]
                if filename.endswith(".gbff.gz"):
                    gb_name = filename
                    break
            if not gb_name:
                raise Exception("Could not find gbff file for {cols[2]}")
            print(gb_name)
            fd_out.write(f"{cols[0]}/{gb_name}\t{cols[3]}")


if __name__ == '__main__':
    main()
