// V2
package grover;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.net.URI;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;

// Hadoop libraries -------------------------------------------------------------------------------------
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
//import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
// End of Hadoop libraries ---------------------------------------------------------------------------------
 
public class Grover {

	private static final long COUNT_PSI = 2; // The number of parallel calculations. The default is the number of maps functions.
	
	public static class Map extends Mapper<LongWritable, Text, LongWritable, DoubleWritable> {
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			String line = value.toString();
			String[] temp = line.split("#");
			long index = Long.parseLong(temp[0]);
			String[] records = temp[1].split(";"); // ";" is the delimiter used in the input file to separate the elements.
			String[] val;
			LongWritable outputKey = new LongWritable();
			DoubleWritable outputValue = new DoubleWritable();
			Configuration conf = context.getConfiguration();
			long N = Long.parseLong(conf.get("N"));
			HashMap<Long, Double> psi = new HashMap<Long, Double>();
			double c1=2.0/N, temp_value=0;
			long j, m=N/2;

			for (long i = 0; i < N; i++) {

				if (i % COUNT_PSI == index) {

					for (String record : records) {

						val = record.split(",");
						j = Long.parseLong(val[0]);
				
						if (j==m) {

							if (i==j) {

								temp_value = 1.0-c1;
							} else {

								temp_value = -c1;
							}
						} else {

							if (i==j) {

								temp_value = c1-1.0;
							} else {

								temp_value = c1;
							}
						}

						if (psi.containsKey(i)) {
							psi.put(i, psi.get(i) + temp_value * Double.parseDouble(val[1]));
						} else {
							psi.put(i, temp_value * Double.parseDouble(val[1]));
						}
					}
				}
				
			}

			for (long i = 0; i < N; i++) {

				if (psi.containsKey(i)) {
					outputKey.set(i);
					outputValue.set(psi.get(i));
					context.write(outputKey, outputValue);
				}			
			}

/*		    
			String line = value.toString();
			String[] records = line.split(","); // "," is the delimiter used in the input file.
			Text outputKey = new Text();
			Text outputValue = new Text();
			Configuration conf = context.getConfiguration();
			long number_elements_A = Long.parseLong(conf.get("number_elements_A")); // Number of elements of the matrix A
			long number_elements_B = Long.parseLong(conf.get("number_elements_B")); // Number of elements of the matrix B
			long number_of_parts = Long.parseLong(conf.get("number_of_parts")); // Number of parts that the matrix A was splitted


			if (number_elements_B <= NUMBER_ELEMENTS_IN_MEMORY) {

				if (records[0].equals("A")) { // A is the left matrix.

					outputKey.set(records[1]);
					outputValue.set("A," + records[2] + "," + records[3] + "," + records[4]);
					context.write(outputKey, outputValue);

				} else {
					if (records[0].equals("B")) { // B is the right matrix.

						outputKey.set(records[1]);
						outputValue.set("B," + records[2] + "," + records[3] + "," +
									records[4]);
						context.write(outputKey, outputValue);

					}
					else {
						if (records[0].equals("#A") || records[0].equals("#B")) { // #A and #B are the header with the matrix dimension.
							
							outputKey.set("-1"); // This key represents the header of the files (both matrices dimensions)
							outputValue.set(records[1] + "," + records[2]);
							context.write(outputKey, outputValue);
						}
					}
				}
			}

			else {
				
				if (records[0].equals("A")) { // A is the left matrix.

					outputKey.set(records[1]);
					outputValue.set("A," + records[2] + "," + records[3] + "," + records[4]);
					context.write(outputKey, outputValue);

				} else {

					if (records[0].equals("B")) { // B is the right matrix.

						long partitionA = Long.parseLong(records[1]) % number_of_parts;

						outputKey.set(Long.toString(partitionA) + "_" + records[1]);
						outputValue.set("B," + records[2] + "," + records[3] + "," +
									records[4]);
						context.write(outputKey, outputValue);

					} else {
						if (records[0].equals("#A") || records[0].equals("#B")) { // #A and #B are the header with the matrix dimension.
						
							outputKey.set("-1"); // This key represents the header of the files (both matrices dimensions)
							outputValue.set(records[1] + "," + records[2]);
							context.write(outputKey, outputValue);
						}
					}
				}
				

			}
*/			
		}
	}
 
	public static class Reduce extends Reducer<LongWritable, DoubleWritable, Text, Text> {
		public void reduce(LongWritable key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {

			for (DoubleWritable val : values){					

				context.write(null, new Text(key.toString() + "," + val.toString()));
			}
/*		
			String[] value, valA = new String[2], valB, coordinatesA = new String[3];
			ArrayList<Text> values_cache = new ArrayList<Text>();
			long number_elements_B = 0;
			boolean findA = false;

			Configuration conf = context.getConfiguration();
			String type_matrix_output = conf.get("type_matrix_output");
			long number_rows_B = Long.parseLong(conf.get("number_rows_B"));
			long number_columns_B = Long.parseLong(conf.get("number_columns_B"));
			long total_number_elements_B = Long.parseLong(conf.get("number_elements_B"));
			Text output = new Text();
			String[] full_key;
			String keyA="", partA="";
			long number_of_parts = (total_number_elements_B / NUMBER_ELEMENTS_IN_MEMORY);
			
			if (!(key.toString().equals("-1"))) {

				if (total_number_elements_B > NUMBER_ELEMENTS_IN_MEMORY) {
					full_key = key.toString().split("_");
					partA = full_key[0];
					keyA = full_key[1];	
				}			
				
				for (Text val : values) {

					number_elements_B++;

					if (!findA) {
						value = val.toString().split(",",2);
						if (value[0].equals("A")) {
							value = val.toString().split(",");
							valA = value[3].split("j");
							coordinatesA[1] = value[1];
							coordinatesA[2] = value[2];
							findA = true;
							number_elements_B--;
						}
					}

					Text writable = new Text(); 
					writable.set(val.toString());
					values_cache.add(writable);		
				}

				if (valA[0] == null && valA[1] == null){
					throw new IOException ("This key haven't a matrix A value");
				}

				for (Text val : values_cache) {
					value = val.toString().split(",");
					if (value[0].equals("B")) {
						valB = value[3].split("j");

						output.set( type_matrix_output + "," +
								Long.toString(
								number_rows_B * Long.parseLong(coordinatesA[1]) + Long.parseLong(value[1])) + "," + // Kronecker row equation
								Long.toString(
								number_columns_B * Long.parseLong(coordinatesA[2]) + Long.parseLong(value[2])) + "," + // Kronecker column equation
								Double.toString(Double.parseDouble(valA[0])*Double.parseDouble(valB[0])) + "j" +
								Double.toString(Double.parseDouble(valA[1])*Double.parseDouble(valB[1])) );

						context.write(null, output);

					}
					
				}

			} else {

				long rows=1, columns=1;
				for (Text val : values) {
					value = val.toString().split(",");
					rows *= Long.parseLong(value[0]);
					columns *= Long.parseLong(value[1]);
				}
				
				context.write(null, new Text("#" + type_matrix_output + "," + Long.toString(rows) + "," +
												Long.toString(columns)));	
			}
*/
		}
	}
	
 
	public static void main(String[] args) throws Exception {

		try{

			Configuration conf = new Configuration();

			//conf.set("type_matrix_output",args[2]); // Set if the output will be matrix type A ou type B

			Path inputPath = new Path(args[0]);
			Path outputPath = new Path(args[1]);

			FileSystem fs_input = FileSystem.get(conf);
			FileStatus[] status = fs_input.listStatus(inputPath); // The names of all files in the input path
			
			for (int i = 0; i < status.length; i++){
				BufferedReader br = new BufferedReader(new InputStreamReader(fs_input.open(status[i].getPath())));
				String old_path = status[i].getPath().toString();
				String new_path = old_path+"_new";
				String line;
				boolean new_size = false, verified = false, first_line=false;
				String[] val;
				line=br.readLine();

				// Empty file. Go to the next.
				if (line == null){
					System.out.println("The input file is empty.");
					continue;
				}


				BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(
									fs_input.create(new Path(new_path),true)));

				while (line != null){
					if (!(line.equals("")) && !(String.valueOf(line.charAt(0)).equals(" "))){
				
						if (!new_size && !verified) {
							verified = true;
							if (line.indexOf("#") == -1) {
								new_size = true;
							} else {
								break;
							}
							
						}

						if (new_size) {
							if (first_line) {
								bw.write(";" + line);
							} else {
								bw.write(line);
								first_line = true;
							}
						}

					}
				
					line=br.readLine();
					
				}
				
				if (new_size) {
					bw.close();
					System.out.println("New format to Psi file. Old format deleted.");
					// Delete the old file that haven't the count.
					fs_input.delete(status[i].getPath(),true);
				}
				else {
					bw.close();
					fs_input.delete(new Path(new_path),true);
				}

				br.close();
				
			}

			status = fs_input.listStatus(inputPath);

			for (int i = 0; i < status.length; i++){
				BufferedReader br = new BufferedReader(new InputStreamReader(fs_input.open(status[i].getPath())));

				String old_path = status[i].getPath().toString();
				String new_path = old_path;
				String line;
				boolean new_size = false, verified = false;
				String[] val;
				line=br.readLine();

				// Empty file. Go to the next.
				if (line == null){
					continue;
				}

				//BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(
				//					fs_input.create(new Path(new_path+"_0"),true)));

				while (line != null){
					if (!(line.equals("")) && !(String.valueOf(line.charAt(0)).equals(" "))){

						//val = line.split(",",2);
				
						if (!new_size && !verified) {							
							verified = true;
							if (line.indexOf("#") == -1) {
								new_size = true;
							} else {
								break;
							}
						}

						if (new_size) {

							//bw.write("0#"+line);
							//bw.close();
							BufferedWriter bw;

							for (long j=0; j < COUNT_PSI; j++) {
								bw = new BufferedWriter(new OutputStreamWriter(
									fs_input.create(new Path(new_path+"_"+Long.toString(j)),true)));
								bw.write(Long.toString(j) + "#" + line);
								bw.close();
							}

						}
					}
				
					line=br.readLine();
					
				}
				
				if (new_size) {
					//bw.close();
					System.out.println("New format 2 to Psi file. Old format deleted.");
					// Delete the old file that haven't the count.
					fs_input.delete(status[i].getPath(),true);
				}
				else {
					//bw.close();
					//fs_input.delete(new Path(new_path),true);
				}

				br.close();
				
			}

			fs_input.close();

			// test HashMap
			//HashMap<Long, Double> psi = new HashMap<Long, Double>();
			//long k = 2;
			//psi.put(k,2.2);
			//if (psi.containsKey(k)) {
			//	psi.put(k, psi.get(k) + 2.6 * 8.0);
			//} else {
			//	psi.put(k, 2.6 * 8.0);
			//}
			//System.out.println("psi="+psi.get(k));

			// args[2] is the value of n
			conf.set("N",Long.toString((long)(Math.pow(2,Integer.parseInt(args[2]))))); // Set the value of N
			//System.out.println("N="+Long.toString((long)(Math.pow(2,Integer.parseInt(args[2])))));

			// Disable the map output compression in Hadoop for gain of performance
			conf.set("mapred.compress.map.output","false");

			//System.out.println("number_elements_A="+Long.toString(COUNT_PSI));
			//System.out.println("number_elements_B="+Long.toString(countB));
			//System.out.println("number_of_parts="+Long.toString(number_of_parts));

			// Set the number of elements of the matrix A and matrix B that are stored in the input files
			//conf.set("number_elements_A",Long.toString(COUNT_PSI));
			//conf.set("number_elements_B",Long.toString(countB));

			
			
			// Set the number of parts that the matrix A was splitted
			//conf.set("number_of_parts",Long.toString(number_of_parts));
			

			FileSystem  fs = FileSystem.get(new URI(outputPath.toString()), conf);		

			// Delete the output directory if it already exists.
			fs.delete(outputPath,true);
			fs.close();

			// Create job
			Job job = new Job(conf, "Grover");
			job.setJarByClass(Grover.class);

			// Specify key / value
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);

			// Setup MapReduce job
			job.setMapperClass(Map.class);
			job.setReducerClass(Reduce.class);

			// Set only the number of reduces tasks
			job.setNumReduceTasks(1);

			// Set only the number of reduces tasks
			//job.setNumReduceTasks(Integer.parseInt(args[3]));

			// Set Map output Key/Value type 
			job.setMapOutputKeyClass(LongWritable.class);
			job.setMapOutputValueClass(DoubleWritable.class);

			//job.setInputFormatClass(TextInputFormat.class);
			job.setInputFormatClass(NLineInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);

			// Input
			FileInputFormat.addInputPath(job, inputPath);

			// Output
			FileOutputFormat.setOutputPath(job, outputPath);

			// Execute job
			job.waitForCompletion(true);


		} catch(Exception e){
			System.out.println(e);
		}

	}
}
