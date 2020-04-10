/*
A KBase module: refseq_importer
*/

module refseq_importer {
    typedef structure {
        string accession;
    } RunSingleImportOutput;

    funcdef run_refseq_importer(mapping<string,UnspecifiedObject> params) returns (UnspecifiedObject output) authentication required;
    funcdef run_single_import(mapping<string,UnspecifiedObject> params) returns (RunSingleImportOutput output) authentication required;
};
