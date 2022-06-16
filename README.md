# testHadoopJava

This project is a test to covert a tabular-like gtf/gff file into noSQL format like the column-family based Hbase(for Hadoop).

The readFromFile class is to read the content of either a gtf or a gff file and write the content into Hbase. By default, four Htables would be created if no table of the same name were created before in the connected database. The four Htables are to record transcripts, genes, exons and other features separately. 

