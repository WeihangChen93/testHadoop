package bdpuh.hbase;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

public class readFromFile {
    /** For both gtf and gff file have same format for first 8 columns, a 
     * common addCol function is written to add these columns into rows. 
     */
    public static void addCol(Put p,String [] arr0) {
        p.addColumn(Bytes.toBytes("Colmn"),Bytes.toBytes("Chrom"), Bytes.toBytes(arr0[0]));
        p.addColumn(Bytes.toBytes("Colmn"),Bytes.toBytes("Feature_type"), Bytes.toBytes(arr0[2]));
        p.addColumn(Bytes.toBytes("Colmn"),Bytes.toBytes("Start"), Bytes.toBytes(arr0[3]));
        p.addColumn(Bytes.toBytes("Colmn"),Bytes.toBytes("End"), Bytes.toBytes(arr0[4]));
        p.addColumn(Bytes.toBytes("Colmn"),Bytes.toBytes("Strand"), Bytes.toBytes(arr0[6]));
    }
    
    public static void main(String[] args) throws IOException{
        read_gtf(args);
    }
    public static void read_gtf(String[] args) throws IOException{
        Configuration conf = HBaseConfiguration.create(new Configuration());
        Connection conn = ConnectionFactory.createConnection(conf);
        Admin hba = conn.getAdmin();
        TableName gtfTable = TableName.valueOf(args[1]);
        //check whether table exists
        if(!hba.tableExists(gtfTable)){
            try {
                HTableDescriptor aT = new HTableDescriptor(gtfTable);
                aT.addFamily(new HColumnDescriptor("Colmn"));
                aT.addFamily(new HColumnDescriptor("Info"));
                hba.createTable(aT);
            } catch (IOException e){}
        } else {
            System.out.println("Table Already exists.Using the existing table now");
        }
        BufferedMutator mutator_gtf= conn.getBufferedMutator(gtfTable);
        // start reading the file
        File f = new File(args[0]);
        BufferedReader br = new BufferedReader(new FileReader(f));
        String line = br.readLine();
        while(line!=null && line.length()!=0){
            if (!line.startsWith("#")){
                String [] arr0 = line.split("\t");
                String rowname = arr0[8];
                Put p = new Put(Bytes.toBytes(rowname));
                addCol(p,arr0);                

                System.out.println("Processing : " + rowname);
                StringTokenizer tokens = new StringTokenizer(arr0[8],";");
                while (tokens.hasMoreTokens()){
                    String [] arr = tokens.nextToken().split("=");
                    if (arr[0].equals("ID")){
                        p.addColumn(Bytes.toBytes("Info"),Bytes.toBytes("ID"),Bytes.toBytes(rowname));
                    }else{
                        p.addColumn(Bytes.toBytes("Info"),Bytes.toBytes(arr[0]),Bytes.toBytes(arr[1]));}
                }
                // put the row into table
                mutator_gtf.mutate(p);

                // update the row 
                line = br.readLine();
            } else {
                line = br.readLine();
            }
        }
        br.close();
        mutator_gtf.close();
    }

    public static void read_gff(String[] args) throws IOException{
        Configuration conf = HBaseConfiguration.create(new Configuration());
        Connection conn = ConnectionFactory.createConnection(conf);
        Admin hba = conn.getAdmin();

        TableName geneTable = TableName.valueOf("gene");
        TableName transcriptTable = TableName.valueOf("transcript");
        TableName exonTable = TableName.valueOf("exon");
        TableName restTable = TableName.valueOf("else");

        if(args.length==1){
            if(!hba.tableExists(transcriptTable)){
                // create table columns
                try {
                    HTableDescriptor gT = new HTableDescriptor(geneTable);
                    HTableDescriptor hT = new HTableDescriptor(transcriptTable);
                    HTableDescriptor eT = new HTableDescriptor(exonTable);
                    HTableDescriptor rT = new HTableDescriptor(restTable);

                    gT.addFamily(new HColumnDescriptor("Colmn"));
                    gT.addFamily(new HColumnDescriptor("Info"));
                    hT.addFamily(new HColumnDescriptor("Colmn"));
                    hT.addFamily(new HColumnDescriptor("Info"));
                    eT.addFamily(new HColumnDescriptor("Colmn"));
                    eT.addFamily(new HColumnDescriptor("Info"));
                    rT.addFamily(new HColumnDescriptor("Colmn"));
                    rT.addFamily(new HColumnDescriptor("Info"));
        
                    hba.createTable(gT);
                    hba.createTable(hT);
                    hba.createTable(eT);
                    hba.createTable(rT);
                } catch (IOException e){
                    System.out.println("New Table Created");
                }
            } else {
                System.out.println("Table Already exists.Using the existing table now");
            }
            
            // create mutator for table
            BufferedMutator mutator_gene = conn.getBufferedMutator(geneTable);
            BufferedMutator mutator_transcript = conn.getBufferedMutator(transcriptTable);
            BufferedMutator mutator_exon = conn.getBufferedMutator(exonTable);
            BufferedMutator mutator_rest = conn.getBufferedMutator(restTable);

            // insert values/row 
            File f = new File(args[0]);
            BufferedReader br = new BufferedReader(new FileReader(f));
            String line = br.readLine();
            int i =1;

            while(line!=null && line.length()!=0){
                if (!line.startsWith("#")){
                    String [] arr0 = line.split("\t");

                    // Create put with id start with “row1”
                    String rowname = "r"+i;
                    Put p = new Put(Bytes.toBytes(rowname));
                    addCol(p,arr0);
                    StringTokenizer tokens = new StringTokenizer(arr0[8],";");
                    while (tokens.hasMoreTokens()){
                        String [] arr = tokens.nextToken().split("=");
                        p.addColumn(Bytes.toBytes("Info"),Bytes.toBytes(arr[0]),Bytes.toBytes(arr[1]));
                    }               
                    // put the row into table
                    if (arr0[2].equals("gene")){
                        mutator_gene.mutate(p);
                    } else if (arr0[2].equals("transcript")){
                        mutator_transcript.mutate(p);
                    } else if (arr0[2].equals("exon")){
                        mutator_exon.mutate(p);
                    } else {
                        mutator_rest.mutate(p);
                    }
                    // update the row 
                    i++;
                    line = br.readLine();
                } else {
                    line = br.readLine();
                }
            }
            br.close();
            mutator_gene.close();
            mutator_transcript.close();
            mutator_exon.close();
            mutator_rest.close();
        } else {
            System.out.println("Please Enter the file name through command line");
        }
    }
}