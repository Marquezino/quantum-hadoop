/*
 * Grover.java	1.1 2015/05/04
 *
 * Copyright (C) 2015 GNU General Public License
 * 
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *                                                                                                                  
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 */


package grover;

import java.io.IOException;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.Iterator;
import java.net.URI;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.OutputCollector;


/**
 *
	This software calculate one step of the Grover's algorithm using Apache Hadoop.
 *
 * @version
	1.1 4 May 2015  * @author
	David Souza  */


public class Grover {

	private static final long COUNT_PSI = 2; // The number of parallel calculations. The default is the number of maps functions.

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
			double c1 = 2.0 / N;
			double temp_value = 0;
			long j;
			long m = N / 2;

			while (values.hasNext()) {
				list_values.add(values.next().toString());
			}

			for (long i = 0; i < N; i++) {

				if (i % COUNT_PSI == index) {

					for (int idx=0; idx < list_values.size(); idx++) {

						val = list_values.get(idx).split(",");
						j = Long.parseLong(val[0]);
				
						if (j == m) {

							if (i == j) {

								temp_value = 1.0 - c1;
							} else {

								temp_value = -c1;
							}
						} else {

							if (i == j) {

								temp_value = c1 - 1.0;
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
		
			for (long i : psi.keySet()) {

				outputValue.set(Long.toString(i) + "," + Double.toString(psi.get(i)));
				output.collect(null, outputValue);
			}

		
		}
	}

	public static void main(String[] args) throws Exception {

		try{

			// Create the Job and set it name
			JobConf conf = new JobConf(Grover.class);
			conf.setJobName("Grover");

			//conf.set("type_matrix_output",args[2]); // Set if the output will be matrix type A ou type B

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
			//job.waitForCompletion(true);
			JobClient.runJob(conf);


		} catch(Exception e){
			System.out.println(e);
		}

	}
}





