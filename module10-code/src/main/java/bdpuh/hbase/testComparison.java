package bdpuh.hbase;
import java.io.*;
import java.util.*;

public class testComparison{
    public static Long startTime;
    public static Long endTime;

    public static ArrayList<String> printSiblings(String transcript_ID,String fileName) throws IOException{
        ArrayList<String> recordArrayList = new ArrayList<String>();
        BufferedReader bufferedReader = new BufferedReader(new FileReader(fileName));
        String s;
        String gene_id ="";
        ArrayList<String> outputArrayList = new ArrayList<String>();
        // find parent gene_id
        while ( (s = bufferedReader.readLine())!=null){
            if (s.contains("ID=ENST")) {
                recordArrayList.add(s);
                if (s.contains( "ID="+transcript_ID) ){
                    String[] arr= s.split("gene_id=");
                    String[] arr1= arr[1].split(";");
                    gene_id = arr1[0];
                }
            }            
        }
        bufferedReader.close();
        // find transcripts with the gene_id
        for (String record : recordArrayList){
            if (record.contains( ("gene_id="+gene_id ) ) && record.contains("ID=ENST") ){
                String[] arr2 = record.split(";");
                String[] arr3 = arr2[0].split("ID=");
                outputArrayList.add(arr3[1]);
            }
        }        
        return outputArrayList;
    }

    public static void createIntron(String transcript_ID,String fileName) throws IOException{
        String s;
        ArrayList<String> recordArrayList = new ArrayList<String>();
        ArrayList<String> outputArrayList = new ArrayList<String>();
        BufferedReader bufferedReader = new BufferedReader(new FileReader(fileName));
        BufferedWriter out = new BufferedWriter(new FileWriter(fileName, true));

        // find parent gene_id
        while ( (s = bufferedReader.readLine())!=null){
            if (s.contains(transcript_ID) && s.contains("exon:") ){
                recordArrayList.add(s);
            }
        }
        bufferedReader.close();
        // calculate start & end of all introns
        String[] arr = new String[9];
        int[] startList=new int[recordArrayList.size()];
        int[] endList = new int[recordArrayList.size()];
        int count = 0;
        for (String record : recordArrayList){
            arr = record.split("\t");
            startList[count] = Integer.valueOf(arr[3]);
            endList[count] = Integer.valueOf(arr[4]);
            count++;
        }        
        // the arraylist does not need to be sorted
        for (int i=0;i<recordArrayList.size()-1;i++){
            Integer intron_start=0;
            Integer intron_end = 0;
            String recd;
            String recd0 = "";
            // check if the strand is + or - 
            if (arr[6].equals("-")){
                intron_start = endList[i+1] + 1 ;
                intron_end = startList[i] -1;                
                for (String kv : (recordArrayList.get(i).split("\t"))[8].split(";") ){
                    if (kv.contains("exon_number")){
                        recd0 = recd0 + "intron_number=" + count + ";";
                    } else if (kv.contains("exon_id")){
                        continue;
                    } else {
                        recd0 = recd0 + kv + ";";
                    }
                }
                recd=arr[0]+"\t"+arr[1]+"\tintron\t"+intron_start+"\t"+intron_end+"\t"+
                     arr[5]+"\t-\t"+arr[7]+"\t" + recd0;
                count--;
            } else {
                // start  = end of this row + 1
                intron_start = endList[i] + 1;
                // end = start of next row - 1
                intron_end= startList[i+1] -1;
                // create intron record string
                for (String kv : (recordArrayList.get(i).split("\t"))[8].replace("exon", "intron").split(";") ){
                    if (kv.contains("intron_id")){
                        continue;
                    } else {
                        recd0 = recd0 + kv + ";";
                    }
                }
                recd=arr[0]+"\t"+arr[1]+"\tintron\t"+intron_start+"\t"+intron_end+"\t"+
                     arr[5]+"\t+\t"+arr[7]+"\t" + recd0;
            }
            //System.out.println(recd);
            outputArrayList.add(recd);

        }        
        for (String record :outputArrayList){
            out.write(record);
        }
        out.close();
    }
    public static void main(String[] args) throws IOException{
        startTime = System.currentTimeMillis();
        createIntron("ENST00000616016.5",args[0]);
        endTime  = System.currentTimeMillis();
        Long t1= endTime-startTime;
        System.out.println("Flatfile Run Time in milliseconds = " + t1 );
    }
}