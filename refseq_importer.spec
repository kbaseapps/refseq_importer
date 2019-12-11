/*
A KBase module: refseq_importer
*/

module refseq_importer {
    typedef structure {
        string report_name;
        string report_ref;
    } ReportResults;

    /*
        This example function accepts any number of parameters and returns results in a KBaseReport
    */
    funcdef run_refseq_importer(mapping<string,UnspecifiedObject> params) returns (ReportResults output) authentication required;

};
