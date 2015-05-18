// V1

package grover;

import java.io.IOException;
import java.util.*;

import java.net.URI;
import org.apache.hadoop.fs.FileSystem;
import org.apache.commons.collections.IteratorUtils;
import org.apache.hadoop.mapreduce.Reducer.Context;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;


public class Grover {

	private static final long COUNT_PSI = 4; // The number of parallel calculations. The default is the number of maps functions.

	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, LongWritable, Text> {

		public void map(LongWritable key, Text value, OutputCollector<LongWritable, Text> output, Reporter reporter) throws IOException {

			LongWritable outputKey = new LongWritable();

			for (long i = 0; i < COUNT_PSI; i++) {
				outputKey.set(i);
				output.collect(outputKey, value);

			}
		}
	}

	public static class Reduce extends MapReduceBase implements Reducer<LongWritable, Text, Text, Text> {
		
		private static Long N;
		public void configure(JobConf job) {
    			N = Long.parseLong(job.get("N"));
		}

		public void reduce(LongWritable key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {			

			long index = Long.parseLong(key.toString());
			String[] val;
			Text outputValue = new Text();
			HashMap<Long, Double> psi = new HashMap<Long, Double>();
			ArrayList<String> list_values = new ArrayList<String>();
			double c1=2.0/N, temp_value=0.0d;
			long j, m=N/2;

			while (values.hasNext()) {
				list_values.add(values.next().toString());
			}

			long i = index;
			while (i < N) {

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

				i+=COUNT_PSI;
				
			}
		
			for (long k: psi.keySet()) {

				outputValue.set(Long.toString(k) + "," + Double.toString(psi.get(k)));
				output.collect(null, outputValue);
			}

		
		}
	}

	public static void main(String[] args) throws Exception {

		try{

			// Create the Job and set it name
			JobConf conf = new JobConf(Grover.class);
			conf.setJobName("Grover");

			Path inputPath = new Path(args[0]);
			Path outputPath = new Path(args[1]);

			// args[2] is the value of n
			conf.set("N",Long.toString((long)(Math.pow(2,Integer.parseInt(args[2]))))); // Set the value of N

			// Disable the map output compression in Hadoop for gain of performance
			conf.set("mapred.compress.map.output","false");

			FileSystem  fs = FileSystem.get(new URI(outputPath.toString()), conf);		

			// Delete the output directory if it already exists.
			fs.delete(outputPath,true);
			fs.close();

			// Specify key / value
			conf.setOutputKeyClass(Text.class);
			conf.setOutputValueClass(Text.class);

			// Setup MapReduce job
			conf.setMapperClass(Map.class);
			conf.setReducerClass(Reduce.class);

			// Set only the number of reduces tasks
			//conf.setNumReduceTasks(2);

			// Set Map output Key/Value type 
			conf.setMapOutputKeyClass(LongWritable.class);
			conf.setMapOutputValueClass(Text.class);

			conf.setInputFormat(TextInputFormat.class);
			conf.setOutputFormat(TextOutputFormat.class);

			// Input
			FileInputFormat.addInputPath(conf, inputPath);

			// Output
			FileOutputFormat.setOutputPath(conf, outputPath);

			// Execute job
			JobClient.runJob(conf);


		} catch(Exception e){
			System.out.println(e);
		}

	}
}





