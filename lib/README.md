# KBase Refseq Importer

## Running

First, generate the `refseq.tsv` file by running the following directly in python:

```sh
cd lib/refseq_importer/utils
python -m list_refseq_genomes
```

When finished, you now have the `refseq.tsv` file in this directory.

Now run the following:

```sh
kb-sdk run refseq_importer.import_from_tsv '{"tsv_path": "/kb/module/lib/refseq_importer/utils/refseq.tsv", "workspace_id": 1234}'
```

Replacing "workspace_id" with the workspace you would like to import into.
