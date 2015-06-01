// V0

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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
// End of Hadoop libraries ---------------------------------------------------------------------------------
 
public class Grover {

	private static final long COUNT_PSI = 2; // The number of parallel calculations. The default is the number of maps functions.
	
	public static class Map extends Mapper<LongWritable, Text, LongWritable, Text> {

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			LongWritable outputKey = new LongWritable();

			for (long i = 0; i < COUNT_PSI; i++) {
				outputKey.set(i);
				context.write(outputKey, value);
			}
			
		}
	}
 
	public static class Reduce extends Reducer<LongWritable, Text, Text, Text> {
		public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			long index = Long.parseLong(key.toString());
			String[] val;
			Text output = new Text();
			Configuration conf = context.getConfiguration();
			long N = Long.parseLong(conf.get("N"));
			HashMap<Long, Double> psi = new HashMap<Long, Double>();
			ArrayList<String> list_values = new ArrayList<String>();
			double c1=2.0/N, temp_value=0;
			long j, m=N/2;

			for (Text value : values) {
				list_values.add(value.toString());
			}

			for (long i = 0; i < N; i++) {

				if (i % COUNT_PSI == index) {

					for (int idx=0; idx < list_values.size(); idx++) {

						val = list_values.get(idx).split(",");
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
		
			for (long i: psi.keySet()) {

				output.set(Long.toString(i) + "," + Double.toString(psi.get(i)));
				context.write(null, output);
			}

		}
	}
	
 
	public static void main(String[] args) throws Exception {

		try{

			Configuration conf = new Configuration();

			Path inputPath = new Path(args[0]);
			Path outputPath = new Path(args[1]);

			// args[2] is the value of n
			conf.set("N",Long.toString((long)(Math.pow(2,Integer.parseInt(args[2]))))); // Set the value of N
			//System.out.println("N="+Long.toString((long)(Math.pow(2,Integer.parseInt(args[2])))));

			// Disable the map output compression in Hadoop for gain of performance
			conf.set("mapred.compress.map.output","false");			

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
			//job.setNumReduceTasks(1);

			// Set Map output Key/Value type 
			job.setMapOutputKeyClass(LongWritable.class);
			job.setMapOutputValueClass(Text.class);

			job.setInputFormatClass(TextInputFormat.class);
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
