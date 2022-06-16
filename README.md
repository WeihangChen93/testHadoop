Column-based nosql database Hbase (Hadoop) practice examples



This project is a test to covert a tabular-like gtf/gff file into noSQL format like the column-family based Hbase(for Hadoop).

***************** requirements *************************
Hadoop 3.2.0+

Hbase 2.4.0+

Java 1.8.0

***************** readFromFile *************************

The readFromFile class is to read the content of a gff file and write the content into Hbase. By default, four Htables would be created if no table of the same name were created before in the connected database. Four Htables are used to record, and also named as, transcripts, genes, exons and other features separately. The ninth column(INFO) of the gff file must be a semi-colon separated list of key value pairs while each key and value is connected with equal sign. 

***************** readHbaseTable ***********************

The readHbaseTable class contains several functions that can read database and do simple manipulation to data such as create or delete records. It is used to search for all annotation records within a target region. Other than connection to hbase, printRegion requires four arguments of , chromosomal number, starting and ending base pair of target region. 

********************************************************

