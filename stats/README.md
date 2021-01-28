
# Import Statistics Scripts

This repository contains some Python scripts for fetching statistics about the
import process. 

We use `poetry` for the Python environment and packages, so with python 3
installed, first run (from within the `./stats` directory):

```sh
pip install poetry
poetry install
```

## Genome counts in the Workspace

From within `./stats`, run:

```sh
WS_TOK=xyz WS_ID=15792 WS_URL=https://ci.kbase.us/services/ws IS_ADMIN=1 ./workspace_stats
```

Where:
* `WS_TOK` is your workspace auth token
* `WS_ID` is the workspace ID you want to query
* `WS_URL` is the url of the workspace server
* `IS_ADMIN` - whether you want to make the request as an admin user

Which will print total counts for each version of the `KBaseGenomes.Genome`
type in the given workspace.

## Local DB stats

The importer keeps a local database of statuses and metadata for every genome
being imported, found in `test_local/workdir/import_state`.

You can generate reports of the state of this database with:

```sh
cd ./stats/
./db_report
```

Some files will get generated:

* `./reports/overview.json` - counts of errors, pending, and finished entries
* `./reports/errors.json` - mapping of Refseq accessions to error messages encountered when trying to import
* `./reports/finished.json` - list of accessions of finished genomes
* `./reports/pending.json` - list of accessions of pending genomes

## Resetting error statuses

If you'd like to retry genomes that had errors, using a certain string pattern
to match the error, then you can use the `./reset_errors` script.

```sh
cd ./stats/
./reset_errors HTTPConnectionPool
```

The above will find every genome with an error message that contains the string
`"HTTPConnectionPool"`, and will reset its status in the database to `pending`.
Next time you run the importer, those genomes will be retried.
