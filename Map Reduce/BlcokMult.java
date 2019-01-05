// INF-553 Homework 1-Block Matrix Multiplication in Hadoop MapReduce
// Name: Penghao Duan ID:7144393984
package www.ehaoopinfo.com;

//Add packages in 'org.apache.hadoop' 
import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.*;
import org.apache.hadoop;
import java.util.HashMap;

public class BlcokMult {
	// Firstly, mapping the entries in Matrix A and B 
    public static class MatrixMapper 
	extends Mapper<LongWritable, Text, Text, Text> {
    	private Text map_key = new Text();
    	private Text map_value = new Text();
		@Override
		public void map(LongWritable key, javax.xml.soap.Text value, Context context)
		    throws IOException, InterruptedException {
			
			public void setup(Context context) throw IOException{
				Configuration conf = context.getConfiguration();
			}
			
			int block_row = 3;
			int block_column = 3;
			
			FileSplit fileSplit = (FileSplit) context.getInputSplit();
			String fileName = fileSplit.getPath().getName();
			
			if (fileName.contains("A")) {
				String input = value.toString().replaceAll("\\(","")
						.replaceAll("\\)","").replaceAll("\\[")
						.replaceAll("\\]","");
				String[] matrixA = input.split(",");
				String[] value_A = new Stirng[matrixA.length-2];
				
				for(int i = 0; i < value_A.length; i++){
					value_A[i] = matrixA[i+2];
				}
				
				for(int b = 0; b < block_row; b++) {
					map_key.set(matrixA[0] + ',' + b);
					map_value.set('A' + "," + matrixA[1] + value_A);
					context.write(map_key, map_value);
				}
			} else if (fileName.contains("B")){
				String input = value.toString().replaceAll("\\(","")
						.replaceAll("\\)","").replaceAll("\\[")
						.replaceAll("\\]","");
				String[] matrixB = input.split(",");
				String[] value_B = new String[matrixB.length-2];
				
				for(int i = 1; i <= value_B.length; i++) {
					value_B[i] = matrixB[i+2];
				}
				
				for(int b = 1; b <= block_column; i++) {
					map_key.set(b + ',' + matrixB[1]);
					map_value.set('B' + "," + matrixB[0]+value_B);
					context.write(map_key, map_value);
				}
			}
		}
    }
    public static class MatrixReducer 
    extends Reducer<Text, Text, Text,Text>){
    		@Override
    		public void reduce(Text key, Iterable<Text> values, Context context)
    				throws IOException, InterruptedException {
    			String[] value;
    			HashMap<Integer, String[]> hashA = new HashMap<Integer, String[]>();
    			HashMap<Integer, String[]> hashB = new HashMap<Integer, String[]>();
    			
    			for(Text val : values) {
    				if(val[0].equals["A"]) {
    					hashA.put(val[1], val[2]);
    				} else {
    					hashB.put(val[1], val[2]);
    				}
    			}
    			int n = 3;
    			String[] a_ij;
    			String[] b_jk;
    			String[] no_value = null;
    			int[] result;
    			
    			for(int j = 1; j <= n; j++) {
    				a_ij = hashA.containsKey(j) ? hashA.get(j) : no_value;
    				b_jk = hashB.containsKey(j) ? hashB.get(j) : no_value;
    				
    				String [] a;
    				String [] b;
    				
    				
    				if(a_ij == null || b_jk == null) {
    					result=new int[]{};
    				}
    				
    				if(a_ij.length == 12) {
    					for (int i = 0; i < 4; i++) {
    						a[i] = a_ij[i];
    					}
    				} else if (a_ij.length == 9) {
    					if(a_ij[0] != "1" && a_ij[1] != "1") {
    						a[0] = "0";
    						a[1] = a_ij[2];
    						a[3] = a_ij[5];
    						a[4] = a_ij[8];
    					} else if (a_ij[3] !="1" && a_ij[4] != "2"){
    						a[0] = a_ij[2];
    						a[1] = "0";
    						a[2] = a_ij[5];
    						a[3] = a_ij[8];
    					} else if (a_ij[6] !="2" && a_ij[7] != "1"){
    						a[0] = a_ij[2];
    						a[1] = a_ij[5];
    						a[2] = "0";
    						a[3] = a_ij[8];
    					} else {
    						a[0] = a_ij[2];
    						a[1] = a_ij[5];
    						a[2] = a_ij[8];
    						a[3] = "0";
    					}
    				} else if (a_ij.length == 6) {
    					if (a_ij[0] == "1" && a_ij[1] =="1" && a_ij[3] == "1" && a_ij[4] == "2") {
    						a[0] = a_ij[2];
    						a[1] = a_ij[5];
    						a[2] = "0";
    						a[3] = "0";
    					} else if (a_ij[0] == "1" && a_ij[1] =="1" && a_ij[3] == "2" && a_ij[4] == "1") {
    						a[0] = a_ij[2];
    						a[1] = "0";
    						a[2] = a_ij[5];
    						a[3] = "0";
    					} else if (a_ij[0] == "1" && a_ij[1] =="1" && a_ij[3] == "2" && a_ij[4] == "2") {
    						a[0] = a_ij[2];
    						a[1] = "0";
    						a[2] = "0";
    						a[3] = a_ij[5];
    					} else if (a_ij[0] == "1" && a_ij[1] =="2" && a_ij[3] == "2" && a_ij[4] == "1") {
    						a[0] = "0";
    						a[1] = a_ij[2];
    						a[2] = a_ij[5];
    						a[3] = "0";
    					} else if (a_ij[0] == "1" && a_ij[1] =="2" && a_ij[3] == "2" && a_ij[4] == "2") {
    						a[0] = "0";
    						a[1] = a_ij[2];
    						a[2] = "0";
    						a[3] = a_ij[5];
    					} else if (a_ij[0] == "2" && a_ij[1] =="1" && a_ij[3] == "2" && a_ij[4] == "2") {
    						a[0] = "0";
    						a[1] = "0";
    						a[2] = a_ij[2];
    						a[3] = a_ij[5];
    					}
    				} else {
    					if (a_ij[0] == "1" && a_ij[1] =="1"){
    						a[0] = a_ij[2];
    						a[1] = "0";
    						a[2] = "0";
    						a[3] = "0";
    					} else if (a_ij[0] == "1" && a_ij[1] =="2") {
    						a[0] = "0";
    						a[1] = a_ij[2];
    						a[2] = "0";
    						a[3] = "0";
    					} else if (a_ij[0] == "2" && a_ij[1] =="1") {
    						a[0] = "0";
    						a[1] = "0";
    						a[2] = a_ij[2];
    						a[3] = "0";
    					} else {
    						a[0] = "0";
    						a[1] = "0";
    						a[2] = "0";
    						a[3] = a_ij[2];
    					}
    				}
    				
    				if(b_jk.length == 12) {
    					for (int i = 0; i < 4; i++) {
    						b[i] = b_jk[i];
    					}
    				} else if (b_jk.length == 9) {
    					if(b_jk[0] != "1" && b_jk[1] != "1") {
    						b[0] = "0";
    						b[1] = b_jk[2];
    						b[3] = b_jk[5];
    						b[4] = b_jk[8];
    					} else if (b_jk[3] !="1" && b_jk[4] != "2"){
    						b[0] = b_jk[2];
    						b[1] = "0";
    						b[2] = b_jk[5];
    						b[3] = b_jk[8];
    					} else if (b_jk[6] !="2" && b_jk[7] != "1"){
    						b[0] = b_jk[2];
    						b[1] = b_jk[5];
    						b[2] = "0";
    						b[3] = b_jk[8];
    					} else {
    						b[0] = b_jk[2];
    						b[1] = b_jk[5];
    						b[2] = b_jk[8];
    						b[3] = "0";
    					}
    				} else if (b_jk.length == 6) {
    					if (b_jk[0] == "1" && b_jk[1] =="1" && b_jk[3] == "1" && b_jk[4] == "2") {
    						b[0] = b_jk[2];
    						b[1] = b_jk[5];
    						b[2] = "0";
    						b[3] = "0";
    					} else if (b_jk[0] == "1" && b_jk[1] =="1" && b_jk[3] == "2" && b_jk[4] == "1") {
    						b[0] = b_jk[2];
    						b[1] = "0";
    						b[2] = b_jk[5];
    						b[3] = "0";
    					} else if (b_jk[0] == "1" && b_jk[1] =="1" && b_jk[3] == "2" && b_jk[4] == "2") {
    						b[0] = b_jk[2];
    						b[1] = "0";
    						b[2] = "0";
    						b[3] = b_jk[5];
    					} else if (b_jk[0] == "1" && b_jk[1] =="2" && b_jk[3] == "2" && b_jk[4] == "1") {
    						b[0] = "0";
    						b[1] = b_jk[2];
    						b[2] = b_jk[5];
    						b[3] = "0";
    					} else if (b_jk[0] == "1" && b_jk[1] =="2" && b_jk[3] == "2" && b_jk[4] == "2") {
    						b[0] = "0";
    						b[1] = b_jk[2];
    						b[2] = "0";
    						b[3] = b_jk[5];
    					} else if (b_jk[0] == "2" && b_jk[1] =="1" && b_jk[3] == "2" && b_jk[4] == "2") {
    						b[0] = "0";
    						b[1] = "0";
    						b[2] = b_jk[2];
    						b[3] = b_jk[5];
    					}
    				} else {
    					if (b_jk[0] == "1" && b_jk[1] =="1"){
    						b[0] = b_jk[2];
    						b[1] = "0";
    						b[2] = "0";
    						b[3] = "0";
    					} else if (b_jk[0] == "1" && b_jk[1] =="2") {
    						b[0] = "0";
    						b[1] = b_jk[2];
    						b[2] = "0";
    						b[3] = "0";
    					} else if (b_jk[0] == "2" && b_jk[1] =="1") {
    						b[0] = "0";
    						b[1] = "0";
    						b[2] = b_jk[2];
    						b[3] = "0";
    					} else {
    						b[0] = "0";
    						b[1] = "0";
    						b[2] = "0";
    						b[3] = b_jk[2];
    					}
    				}
    				
    				result[0] = Integer.parseInt(a[0]) * Integer.parseInt(b[0]) +
    						Integer.parseInt(a[1]) * Integer.parseInt(b[2]);
    				result[1] = Integer.parseInt(a[0]) * Integer.parseInt(b[1]) +
    						Integer.parseInt(a[1]) * Integer.parseInt(b[3]);
    				result[2] = Integer.parseInt(a[2]) * Integer.parseInt(b[0]) +
    						Integer.parseInt(a[3]) * Integer.parseInt(b[3]);
    				result[3] = Integer.parseInt(a[2]) * Integer.parseInt(b[1]) +
    						Integer.parseInt(a[3]) * Integer.parseInt(b[3]);
    				
    				
    				/*String final_value;
    				
    				if (result[0] == 0) {
    					final_value ="(1,2,"+ result[0]+")"+;
    				}*/
    			}
    			
    			if(result.length == 4){
    				context.write(null, new Text(key.toString() + "," + Integer.toString(result[0])+
    						Integer.toString(result[1]) + "," + Integer.toString(result[2]) + "," +
    						Integer.toString(result[3])));
    			}
    		}
    }
    
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 3) {
            System.err.println("error");
            System.exit(2);
        }

        Job job = new Job(conf, "average");
        job.setJarByClass(BlockMult.class);
        job.setMapperClass(Map1.class);
        job.setMapperClass(Map2.class);
        job.setReducerClass(Reduce.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(MapWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        job.setJarByClass(MultipleInputs.class);
//        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
//        for (int i = 0; i < otherArgs.length - 1; ++i) {
//            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
//        }
        Path p1=new Path(otherArgs[0]);
        Path p2=new Path(otherArgs[1]);
        MultipleInputs.addInputPath(job, p1, TextInputFormat.class, Map1.class);
        MultipleInputs.addInputPath(job, p2, TextInputFormat.class, Map2.class);
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
    /*public class MatrixMultiply {

        public static void main(String[] args) throws Exception {
            if (args.length != 2) {
                System.err.println("Usage: MatrixMultiply <in_dir> <out_dir>");
                System.exit(2);
            }
            @SuppressWarnings("deprecation")
            Job job = new Job(conf, "MatrixMultiply");
		    job.setJarByClass(MatrixMultiply.class);
		    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(Text.class);
		
		    job.setMapperClass(Map.class);
		    job.setReducerClass(Reduce.class);
		
		    job.setInputFormatClass(TextInputFormat.class);
		    job.setOutputFormatClass(TextOutputFormat.class);
		
		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		    job.waitForCompletion(true);
        }
    }*/
}

