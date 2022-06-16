package bdpuh.hbase;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

public class readHbaseTable {

    public static ArrayList<Result> printRegion(Connection conn,String chrom,
                                   String start, String end) throws IOException {
        ArrayList<Result> outputArrayList = new ArrayList<Result>();
        // get a list of all tables in the HBase
        String[] tbNames = {"appris_functional_residue","Ensembl_Regulatory_Build_20190329",
                            "gene","exon","intron","UTR"};

        for(String tbN : tbNames) {
            TableName TABLE_0 = TableName.valueOf(tbN);
            Table tb = conn.getTable(TABLE_0);
            System.out.println("Getting table : " + tbN);
            Scan scan = new Scan();
            List<Filter> filters = new ArrayList<Filter>();

            SingleColumnValueFilter filter1 = new SingleColumnValueFilter(Bytes.toBytes("Colmn"),Bytes.toBytes("Chrom"),
            CompareOperator.EQUAL, new BinaryComparator(Bytes.toBytes(chrom)));

            // SingleColumnValueFilter filter2 = new  SingleColumnValueFilter(Bytes.toBytes("Colmn"),Bytes.toBytes("Start"),
            // CompareOperator.LESS_OR_EQUAL, new BinaryComparator(Bytes.toBytes(start)));                                

            // SingleColumnValueFilter filter3 = new  SingleColumnValueFilter(Bytes.toBytes("Colmn"),Bytes.toBytes("End"),
            // CompareOperator.GREATER_OR_EQUAL, new BinaryComparator(Bytes.toBytes(end)));

            // // set filters
            filters.add(filter1);
            // filters.add(filter2);
            // filters.add(filter3);
            FilterList filterList1 = new FilterList(FilterList.Operator.MUST_PASS_ALL, filters);
            scan.setFilter(filterList1);
            ResultScanner scanner1 = tb.getScanner(scan);
            
            int count =0;
            for (Result rr : scanner1) {
                count++;
                Result row = tb.get(new Get(rr.getRow()));
                String transcript_id = Bytes.toString(row.getValue(Bytes.toBytes("Info"),Bytes.toBytes("transcript_id")));
                Integer strt=Bytes.toInt(row.getValue(Bytes.toBytes("Colmn"),Bytes.toBytes("Start")));
                Integer ed = Bytes.toInt(row.getValue(Bytes.toBytes("Colmn"),Bytes.toBytes("End")));                
                if (strt >= Integer.valueOf(start) && ed <= Integer.valueOf(end)){
                    System.out.print("Found transcript: " + transcript_id + "|");
                    outputArrayList.add(row);
                }
            }
            scanner1.close();
        }
        System.out.println("");
        // System.out.print("Find  " + count + " record with scan");
        return outputArrayList;
    }

    public static ArrayList<String> printSiblings(Connection conn,String transcriptID) throws IOException{
        // String transcriptID = "ENST00000437963.5";
        ArrayList<String> outputArrayList = new ArrayList<String>();
        // Get the table
        TableName TABLE_transcript = TableName.valueOf("transcript");
        Table tableTranscript = conn.getTable(TABLE_transcript);            
        // get scan by ID 
        Scan scan = new Scan();
        ValueFilter filter1 = new ValueFilter(CompareFilter.CompareOp.EQUAL,
                                                new BinaryComparator(Bytes.toBytes(transcriptID)));
        scan.setFilter(filter1);
        // get target row use scan 
        ResultScanner scanner = tableTranscript.getScanner(scan);
        String rowkey = "";
        for (Result rr : scanner) {
            rowkey = Bytes.toString(rr.getRow());
        }
        if (rowkey.length()>1){
            Result result = tableTranscript.get(new Get(Bytes.toBytes(rowkey)));
            String gene_id = Bytes.toString(result.getValue(Bytes.toBytes("Info"), 
                                                            Bytes.toBytes("Parent")));
            // get other transcript with same Parent 
            System.out.println("gene_id : " + gene_id);
            Scan scan2 = new Scan();
            ValueFilter filter2 = new ValueFilter(CompareFilter.CompareOp.EQUAL,
                                                new BinaryComparator(Bytes.toBytes(gene_id)));
            scan2.setFilter(filter2);
            ResultScanner scanner2 = tableTranscript.getScanner(scan2);
            for (Result rr : scanner2) {
                Result transcript_row = tableTranscript.get(new Get(rr.getRow()));   
                String ID = Bytes.toString(transcript_row.getValue(Bytes.toBytes("Info"), Bytes.toBytes("ID")));
                outputArrayList.add(ID);
            }
        }        
        return outputArrayList;
    }
    
    public static void createIntron(Connection conn,String transcriptID) throws IOException{
        // create object 
        Admin hba = conn.getAdmin();
        // if intron table not exist, create the table 
        TableName intronTable = TableName.valueOf("intron");
        if (!hba.tableExists(intronTable)){
            HTableDescriptor iT = new HTableDescriptor(intronTable);
            iT.addFamily(new HColumnDescriptor("Colmn"));
            iT.addFamily(new HColumnDescriptor("Info"));
            hba.createTable(iT);
        }
        BufferedMutator mutator_intron = conn.getBufferedMutator(intronTable);

        //find all exons by transcriptID in exonTable
        TableName TABLE_exon = TableName.valueOf("exon");
        Table tableExon = conn.getTable(TABLE_exon);
        // get all exons by transcript ID 
        Scan scan = new Scan();
        ValueFilter filter1 = new ValueFilter(CompareFilter.CompareOp.EQUAL,
                                                new BinaryComparator(Bytes.toBytes(transcriptID)));
        scan.setFilter(filter1);
        // only certain columns will be write into intron table
        ArrayList<String> rowKeybList = new ArrayList<String>();
        ArrayList<String> unsorted_startList=new ArrayList<String>();
        ArrayList<String> unsorted_endList = new ArrayList<String>();
        ArrayList<String> unsorted_exonNumbList = new ArrayList<String>();
        String strand = ".";
        String chrom = "chr";
        String gene_id="";
        String gene_name="";
        String gene_type="";
        String tag = "";
        String level = "";
        String transcript_support_level = "";
        // count how many rows in the scan
        ResultScanner scanner = tableExon.getScanner(scan);
        int count = 0;
        for (Result rr : scanner) {
            Result exon_row = tableExon.get(new Get(rr.getRow()));
            String rk = Bytes.toString(rr.getRow());
            rowKeybList.add(rk);

            String Strt = Bytes.toString(exon_row.getValue(Bytes.toBytes("Colmn"),Bytes.toBytes("Start")));
            unsorted_startList.add(Strt);

            String end = Bytes.toString(exon_row.getValue(Bytes.toBytes("Colmn"),Bytes.toBytes("End")));
            unsorted_endList.add(end);

            String eNb = Bytes.toString(exon_row.getValue(Bytes.toBytes("Info"), Bytes.toBytes("exon_number")));
            unsorted_exonNumbList.add(eNb);

            if (count == 0){
                chrom = Bytes.toString(exon_row.getValue(Bytes.toBytes("Colmn"),Bytes.toBytes("Chrom")));
                strand =Bytes.toString(exon_row.getValue(Bytes.toBytes("Colmn"),Bytes.toBytes("Strand")));
                gene_id=Bytes.toString(exon_row.getValue(Bytes.toBytes("Info"),Bytes.toBytes("gene_id")));
                gene_name = Bytes.toString(exon_row.getValue(Bytes.toBytes("Info"),Bytes.toBytes("gene_name")));
                gene_type = Bytes.toString(exon_row.getValue(Bytes.toBytes("Info"),Bytes.toBytes("gene_type")));
                tag = Bytes.toString(exon_row.getValue(Bytes.toBytes("Info"),Bytes.toBytes("tag")));
                level = Bytes.toString(exon_row.getValue(Bytes.toBytes("Info"),Bytes.toBytes("level")));
                transcript_support_level= Bytes.toString(exon_row.getValue(Bytes.toBytes("Info"),Bytes.toBytes("transcript_support_level")));
            }
            count++;
        }
        // create record if more than one exon per transcript
        if (count > 1){
            // This block is used to sort the  mapreduce result of scanned rows
            int siz = rowKeybList.size();
            int[] rowKey = new int[siz];
            int[] startList=new int[siz];
            String[] exonNumbList = new String[siz];
            int[] endList = new int[siz];
            ArrayList<Integer> sorted_rowKey=new ArrayList<Integer>();
            // sort the row key 
            for (int i=0;i<rowKeybList.size();i++){
                int r = Integer.valueOf(rowKeybList.get(i).replace("r", ""));
                rowKey[i] = r;
                sorted_rowKey.add(Integer.valueOf(rowKeybList.get(i).replace("r", "")));
            }
            sorted_rowKey.sort(Comparator.naturalOrder());
            
            for (int i = 0; i<sorted_rowKey.size(); i++){
                int indx = sorted_rowKey.indexOf(rowKey[i]);
                startList[indx] = Integer.valueOf(unsorted_startList.get(i));
                endList[indx] = Integer.valueOf(unsorted_endList.get(i));
                exonNumbList[indx] = unsorted_exonNumbList.get(i);
            }

            /** This loop is used to put record into the intron table.
             * Since scan do not have hasNext/next method, this for loop is 
             * written with nested for{if{}} loop*/ 
            for (int i=0;i<exonNumbList.length-1;i++){
                Integer intron_start=0;
                Integer intron_end = 0;
                // check if the strand is + or - 
                if (strand.equals("-")){
                    intron_start = endList[i+1] + 1 ;
                    intron_end = startList[i] -1;
                } else {
                    // start  = end of this row + 1
                    intron_start = endList[i] + 1;
                    // end = start of next row - 1
                    intron_end= startList[i+1] -1;
                }
                // intron number (some transcript mark the exon on last row as first)
                // check if it is true for this record
                int intronNumber;
                if (Integer.valueOf(exonNumbList[0]).equals(1)){
                    intronNumber = i + 1;
                } else {
                    intronNumber = Integer.valueOf(exonNumbList[i]) - 1;
                }
                String N = intronNumber + "";
                // ID
                String intron_id = "intron:"+transcriptID+":"+intronNumber;
                System.out.println(intron_id);
                // Create put with id start with “row1”
                String rowname = intron_id;
                Put p = new Put(Bytes.toBytes(rowname));

                p.addColumn(Bytes.toBytes("Colmn"),Bytes.toBytes("Chrom"),Bytes.toBytes(chrom));
                p.addColumn(Bytes.toBytes("Colmn"),Bytes.toBytes("Start"),Bytes.toBytes(intron_start.toString()));
                p.addColumn(Bytes.toBytes("Colmn"),Bytes.toBytes("Feature_type"),Bytes.toBytes("intron"));
                p.addColumn(Bytes.toBytes("Colmn"),Bytes.toBytes("End"),Bytes.toBytes(intron_end.toString()));
                p.addColumn(Bytes.toBytes("Colmn"),Bytes.toBytes("Strand"),Bytes.toBytes(strand));

                p.addColumn(Bytes.toBytes("Info"),Bytes.toBytes("ID"),Bytes.toBytes(intron_id));
                p.addColumn(Bytes.toBytes("Info"),Bytes.toBytes("Parent"),Bytes.toBytes(transcriptID));
                p.addColumn(Bytes.toBytes("Info"),Bytes.toBytes("intron_number"),Bytes.toBytes(N)); 

                try {
                    if (gene_id != null) {p.addColumn(Bytes.toBytes("Info"),Bytes.toBytes("gene_id"),Bytes.toBytes(gene_id));}
                    if (gene_name != null) {p.addColumn(Bytes.toBytes("Info"),Bytes.toBytes("gene_name"),Bytes.toBytes(gene_name));}
                    if (gene_type != null) {p.addColumn(Bytes.toBytes("Info"),Bytes.toBytes("gene_type"),Bytes.toBytes(gene_type));}                
                    if (level != null) {p.addColumn(Bytes.toBytes("Info"),Bytes.toBytes("level"),Bytes.toBytes(level));}
                    if (tag != null) {p.addColumn(Bytes.toBytes("Info"),Bytes.toBytes("tag"),Bytes.toBytes(tag));}
                    if (transcript_support_level != null) {p.addColumn(Bytes.toBytes("Info"),Bytes.toBytes("transcript_support_level"),
                                                                           Bytes.toBytes(transcript_support_level));}
                } catch (NullPointerException e){}
                mutator_intron.mutate(p);
            }
        }
        mutator_intron.close();
    }

    public static void createUTR(Connection conn,String transcript_ID) throws IOException{
        // unify UTR in the record
        Admin hba = conn.getAdmin();

        // if intron table not exist, create the table like other 3
        TableName utrTable = TableName.valueOf("UTR");
        if (!hba.tableExists(utrTable)){
            HTableDescriptor uT = new HTableDescriptor(utrTable);
            uT.addFamily(new HColumnDescriptor("Colmn"));
            uT.addFamily(new HColumnDescriptor("Info"));
            hba.createTable(uT);
        }
        BufferedMutator mutator_UTR = conn.getBufferedMutator(utrTable);

        // get all known UTRs from TABLE_else
        TableName TABLE_else = TableName.valueOf("else");
        Table tableElse = conn.getTable(TABLE_else);
        // get all row in else by transcript ID 
        Scan scan = new Scan();
        ValueFilter filter1 = new ValueFilter(CompareFilter.CompareOp.EQUAL,
                                                new BinaryComparator(Bytes.toBytes(transcript_ID)));
        
        // set variables
        String gene_type ="";
        String transcript_type ="";
        String strand = ".";
        String chrom = "chr";
        String gene_id="";
        String gene_name="";
        String tag = "";
        String level = "";
        String transcript_support_level = "";
        ArrayList<String> rowKeybList = new ArrayList<String>();
        ArrayList<String> unsorted_startList=new ArrayList<String>();
        ArrayList<String> unsorted_endList = new ArrayList<String>();
        ArrayList<String> unsorted_feature_typeList=new ArrayList<String>();
        // read scanned content into ArrayLists
        ResultScanner scanner = tableElse.getScanner(scan);
        int count = 0;
        for (Result rr : scanner) {
            Result else_row = tableElse.get(new Get(rr.getRow()));
            String rk = Bytes.toString(rr.getRow());
            rowKeybList.add(rk);

            String Strt = Bytes.toString(else_row.getValue(Bytes.toBytes("Colmn"),Bytes.toBytes("Start")));
            unsorted_startList.add(Strt);

            String end = Bytes.toString(else_row.getValue(Bytes.toBytes("Colmn"),Bytes.toBytes("End")));
            unsorted_endList.add(end);

            String ftp = Bytes.toString(else_row.getValue(Bytes.toBytes("Colmn"),Bytes.toBytes("Feature_type")));
            unsorted_feature_typeList.add(ftp);

            if (count == 0){
                chrom = Bytes.toString(else_row.getValue(Bytes.toBytes("Colmn"),Bytes.toBytes("Chrom")));
                strand =Bytes.toString(else_row.getValue(Bytes.toBytes("Colmn"),Bytes.toBytes("Strand")));
                gene_id=Bytes.toString(else_row.getValue(Bytes.toBytes("Info"),Bytes.toBytes("gene_id")));
                gene_name = Bytes.toString(else_row.getValue(Bytes.toBytes("Info"),Bytes.toBytes("gene_name")));
                gene_type = Bytes.toString(else_row.getValue(Bytes.toBytes("Info"),Bytes.toBytes("gene_type")));
                transcript_type=Bytes.toString(else_row.getValue(Bytes.toBytes("Info"),Bytes.toBytes("transcript_type")));
                tag = Bytes.toString(else_row.getValue(Bytes.toBytes("Info"),Bytes.toBytes("tag")));
                level = Bytes.toString(else_row.getValue(Bytes.toBytes("Info"),Bytes.toBytes("level")));
                transcript_support_level= Bytes.toString(else_row.getValue(Bytes.toBytes("Info"),Bytes.toBytes("transcript_support_level")));
            }
            count++;
        }
       
        // test if transcript have UTR at all
        // by value in gene_type & transcript_type
        if (unsorted_feature_typeList.contains("five_prime_UTR")||unsorted_feature_typeList.contains("three_prime_UTR")){
            Boolean run = true;
            ArrayList<String> gene_type_list = new ArrayList<>(Arrays.asList(
                "protein_coding","polymorphic_pseudogene","TEC"));
            ArrayList<String> transcript_type_list = new ArrayList<>(Arrays.asList(
                "protein_coding","polymorphic_pseudogene","nonsense_mediated_decay","TEC"));
            if(! gene_type.equals(transcript_type)){
                run = false;
                if (gene_type_list.contains(gene_type)){
                    if (transcript_type_list.contains(transcript_type)){
                        run = true;
                    }
                }
            }
            if (gene_type.contains("RNA")){run=false;}
            if (run){
                    // start sorting the arraylists 
                int siz = rowKeybList.size();
                int[] rowKey = new int[siz];
                int[] startList=new int[siz];
                String[] feature_typeList = new String[siz];
                int[] endList = new int[siz];
                ArrayList<Integer> sorted_rowKey=new ArrayList<Integer>();

                // sort the row key 
                for (int i=0;i<rowKeybList.size();i++){
                    int r = Integer.valueOf(rowKeybList.get(i).replace("r", ""));
                    rowKey[i] = r;
                    sorted_rowKey.add(Integer.valueOf(rowKeybList.get(i).replace("r", "")));
                }
                sorted_rowKey.sort(Comparator.naturalOrder());

                for (int i = 0; i<sorted_rowKey.size(); i++){
                    int indx = sorted_rowKey.indexOf(rowKey[i]);
                    startList[indx] = Integer.valueOf(unsorted_startList.get(i));
                    endList[indx] = Integer.valueOf(unsorted_endList.get(i));
                    feature_typeList[indx] = unsorted_feature_typeList.get(i);
                }

                // find index of prime_UTR
                LinkedList<Integer>UTR3_ind = new LinkedList<>();
                LinkedList<Integer>UTR5_ind = new LinkedList<>();
                for (int i=0;i<feature_typeList.length;i++){
                    if (feature_typeList[i].equals("five_prime_UTR")){
                        UTR5_ind.add(i);
                    } else if (feature_typeList[i].equals("three_prime_UTR")){
                        UTR3_ind.add(i);
                    }
                }
                // for 
                String ID_UTR5 = "UTR5"+transcript_ID;
                Integer UTR5_start=0;
                Integer UTR5_end=0;
                if (UTR5_ind.size()!=0){
                    Put p5 = new Put(Bytes.toBytes(ID_UTR5));
                    if (UTR5_ind.size()>1) {
                        if (startList[UTR5_ind.getFirst()] < startList[UTR5_ind.getLast()]){
                            UTR5_start = startList[UTR5_ind.getFirst()];
                            UTR5_end = endList[UTR5_ind.getLast()];
                        } else {
                            UTR5_start = startList[UTR5_ind.getLast()];
                            UTR5_end = endList[UTR5_ind.getFirst()];
                        }
                    }  
                    if (UTR5_ind.size()==1) {
                        UTR5_start = startList[ UTR5_ind.getFirst()];
                        UTR5_end = endList[ UTR5_ind.getFirst()];
                    }
                    p5.addColumn(Bytes.toBytes("Colmn"),Bytes.toBytes("Chrom"),Bytes.toBytes(chrom));
                    p5.addColumn(Bytes.toBytes("Colmn"),Bytes.toBytes("Start"),Bytes.toBytes(UTR5_start.toString()));
                    p5.addColumn(Bytes.toBytes("Colmn"),Bytes.toBytes("Feature_type"),Bytes.toBytes("UTR5"));
                    p5.addColumn(Bytes.toBytes("Colmn"),Bytes.toBytes("End"),Bytes.toBytes(UTR5_end.toString()));
                    p5.addColumn(Bytes.toBytes("Colmn"),Bytes.toBytes("Strand"),Bytes.toBytes(strand));
                    p5.addColumn(Bytes.toBytes("Info"),Bytes.toBytes("ID"),Bytes.toBytes(ID_UTR5));
                    p5.addColumn(Bytes.toBytes("Info"),Bytes.toBytes("Parent"),Bytes.toBytes(transcript_ID));
                    try {
                        if (gene_id != null) {p5.addColumn(Bytes.toBytes("Info"),Bytes.toBytes("gene_id"),Bytes.toBytes(gene_id));}
                        if (gene_name != null) {p5.addColumn(Bytes.toBytes("Info"),Bytes.toBytes("gene_name"),Bytes.toBytes(gene_name));}
                        if (gene_type != null) {p5.addColumn(Bytes.toBytes("Info"),Bytes.toBytes("gene_type"),Bytes.toBytes(gene_type));}
                        if (transcript_type!=null){p5.addColumn(Bytes.toBytes("Info"),Bytes.toBytes("gene_type"),Bytes.toBytes(transcript_type));}
                        if (level != null) {p5.addColumn(Bytes.toBytes("Info"),Bytes.toBytes("level"),Bytes.toBytes(level));}
                        if (tag != null) {p5.addColumn(Bytes.toBytes("Info"),Bytes.toBytes("tag"),Bytes.toBytes(tag));}
                        if (transcript_support_level != null) {p5.addColumn(Bytes.toBytes("Info"),Bytes.toBytes("transcript_support_level"),
                                                                            Bytes.toBytes(transcript_support_level));}
                    } catch (NullPointerException e){}
                    mutator_UTR.mutate(p5);
                }

                String ID_UTR3 = "UTR3"+transcript_ID;
                Integer UTR3_start;
                Integer UTR3_end;
                if (UTR3_ind.size()!=0){
                    Put p3 = new Put(Bytes.toBytes(ID_UTR3));
                    if (UTR3_ind.size()!=1){
                        if (startList[UTR3_ind.getFirst()] < startList[UTR3_ind.getLast()]){
                            UTR3_start = startList[UTR3_ind.getFirst()];
                            UTR3_end = endList[UTR3_ind.getLast()];
                        } else {
                            UTR3_start = startList[UTR3_ind.getLast()];
                            UTR3_end = endList[UTR3_ind.getFirst()];
                        }
                    } else {
                        UTR3_start = startList[ UTR3_ind.getFirst()];
                        UTR3_end = endList[ UTR3_ind.getFirst()];
                    }
                    p3.addColumn(Bytes.toBytes("Colmn"),Bytes.toBytes("Chrom"),Bytes.toBytes(chrom));
                    p3.addColumn(Bytes.toBytes("Colmn"),Bytes.toBytes("Start"),Bytes.toBytes(UTR3_start.toString()));
                    p3.addColumn(Bytes.toBytes("Colmn"),Bytes.toBytes("Feature_type"),Bytes.toBytes("UTR3"));
                    p3.addColumn(Bytes.toBytes("Colmn"),Bytes.toBytes("End"),Bytes.toBytes(UTR3_end.toString()));
                    p3.addColumn(Bytes.toBytes("Colmn"),Bytes.toBytes("Strand"),Bytes.toBytes(strand));

                    p3.addColumn(Bytes.toBytes("Info"),Bytes.toBytes("ID"),Bytes.toBytes(ID_UTR3));
                    p3.addColumn(Bytes.toBytes("Info"),Bytes.toBytes("Parent"),Bytes.toBytes(transcript_ID));
                    try {
                        if (gene_id != null) {p3.addColumn(Bytes.toBytes("Info"),Bytes.toBytes("gene_id"),Bytes.toBytes(gene_id));}
                        if (gene_name != null) {p3.addColumn(Bytes.toBytes("Info"),Bytes.toBytes("gene_name"),Bytes.toBytes(gene_name));}
                        if (gene_type != null) {p3.addColumn(Bytes.toBytes("Info"),Bytes.toBytes("gene_type"),Bytes.toBytes(gene_type));}
                        if (transcript_type!=null){p3.addColumn(Bytes.toBytes("Info"),Bytes.toBytes("gene_type"),Bytes.toBytes(transcript_type));}
                        if (level != null) {p3.addColumn(Bytes.toBytes("Info"),Bytes.toBytes("level"),Bytes.toBytes(level));}
                        if (tag != null) {p3.addColumn(Bytes.toBytes("Info"),Bytes.toBytes("tag"),Bytes.toBytes(tag));}
                        if (transcript_support_level != null) {p3.addColumn(Bytes.toBytes("Info"),Bytes.toBytes("transcript_support_level"),
                                                                            Bytes.toBytes(transcript_support_level));}
                    } catch (NullPointerException e){}
                    mutator_UTR.mutate(p3);
                }
            }
        }
    }

    public static void deleteTranscript(Connection conn,String transcriptID) throws IOException{
        /** Check whether this transcript is not the only child of the gene,
         *  if so, it is more suitable to keep the transcript anyway.
        */
        ArrayList<String> transcriptIDList = printSiblings(conn,transcriptID);
        
        if (transcriptIDList!=null && transcriptIDList.size()>1){            
            System.out.println("more than 1 transcript/gene");
            // remove the transcriptID from the table
            String[] tbNames = {"transcript","exon","else","intron","UTR"};
            for(String tb : tbNames) {
                // delete from all tables
                deleteByColVal(conn,tb,"Info","transcript_id",transcriptID,false);
                
            }
        } else {
            System.out.println("Invalid transcript : " + transcriptID);
        }
    }

    public static void deleteByColVal(Connection conn,String tableName,String famName,String colName,
                                      String substr,Boolean regex) throws IOException{
        Admin hba = conn.getAdmin();
        int count = 0;
        if (hba.tableExists(TableName.valueOf(tableName))){
            Table tableDEL = conn.getTable(TableName.valueOf(tableName));
            Scan scan = new Scan();
            SingleColumnValueFilter filter1;
            if (regex.equals(false)){
                filter1 = new SingleColumnValueFilter(Bytes.toBytes(famName),Bytes.toBytes(colName),
                                                                            CompareFilter.CompareOp.EQUAL,
                                                                            new SubstringComparator(substr));
            } else {
                filter1 = new SingleColumnValueFilter(Bytes.toBytes(famName),Bytes.toBytes(colName),
                                                                            CompareFilter.CompareOp.EQUAL,
                                                                            new RegexStringComparator(substr));
            }
            scan.setFilter(filter1);
            ResultScanner scannerDEL = tableDEL.getScanner(scan);
            for (Result rr : scannerDEL) {         
                count++;       
                Delete del = new Delete(rr.getRow());
                Result transcript_row = tableDEL.get(new Get(rr.getRow())); 
                String ID = Bytes.toString(transcript_row.getValue(Bytes.toBytes("Info"), Bytes.toBytes("ID")));
                System.out.println("Deleting : " + ID);
                tableDEL.delete(del);
            }
            System.out.println(count + " rows deleted");
            scannerDEL.close();
        } else{System.out.println("Table "+ tableName +" not exist!");}
    }
    public static void main(String[] args) throws IOException{
        Configuration config = HBaseConfiguration.create(new Configuration());
        Connection con = ConnectionFactory.createConnection(config);

        // File f = new File(args[0]);
        // BufferedReader br = new BufferedReader(new FileReader(f));

        // String line = br.readLine();
        // while (line!=null && line.length()!=0){
        //     createIntron(con,line);
        //     line = br.readLine();
        // }
        // br.close();
        Long startTime = System.currentTimeMillis();
        createIntron(con,"ENST00000616016.5");
        Long endTime  = System.currentTimeMillis();
        Long t1= endTime-startTime;
        System.out.println("Hbase Run Time in milliseconds = " + t1 );
    }

}