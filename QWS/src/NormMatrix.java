/*
 * NormMatrix.java	1.0 2015/03/10
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


package mult;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.ReflectionUtils;


/**
 *
	This software calculate the matrix norm using Apache Hadoop.
 *
 * @version
	1.0 10 Mar 2015  * @author
	David Souza  */

 
public class NormMatrix {
 
	public static class Map extends Mapper<LongWritable, Text, LongWritable, DoubleWritable> {
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		    
			String line = value.toString();
			String[] records = line.split(",");	// "," is the delimiter used in the input file.
			DoubleWritable outputReal = new DoubleWritable();
			DoubleWritable outputImaginary = new DoubleWritable();
			double[] element = new double[2];
			String[] vals;

			if (records[0].indexOf("#") == -1) {	// # is the header of the matrix file.

				vals = records[3].split("j");
				element[0] = Double.parseDouble(vals[0]);
				element[1] = Double.parseDouble(vals[1]);
				outputReal.set(element[0] * element[0]);
				outputImaginary.set(element[1] * element[1]);

				context.write(new LongWritable(0), outputReal);
				context.write(new LongWritable(1), outputImaginary);

			}
		}

	}


	public static class Combine extends Reducer<LongWritable, DoubleWritable, LongWritable, DoubleWritable> {

		public void reduce(LongWritable key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
			
			DoubleWritable output = new DoubleWritable();
			double sum = 0.0d;

			for (DoubleWritable val : values) {
				sum += val.get();
			}

			output.set(sum);

			context.write(key, output);

		}
	}

 
	public static class Reduce extends Reducer<LongWritable, DoubleWritable, Text, Text> {
		public void reduce(LongWritable key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
			
			double sum = 0.0d;

			if (key.toString().equals("0")) {	// 0 is the real part of the number

				for (DoubleWritable val : values) {
					sum += val.get();
				}

				context.write(null, new Text("Real=" + Double.toString(Math.sqrt(sum))));	
			
			} else {

				if (key.toString().equals("1")) {	// 1 is the imaginary part of the number

					for (DoubleWritable val : values) {
						sum += val.get();
					}

					context.write(null, new Text("Imaginary=" + Double.toString(Math.sqrt(sum))));		
			
				}

			}

		}
	}
	
 
	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		Path inputPath;
		Path outputPath;
		FileSystem  fs;
		Job job;

		try{

			inputPath = new Path(args[0]);
			outputPath = new Path(args[1]);

			fs = FileSystem.get(new URI(outputPath.toString()), conf);		

			// Delete the output directory if it already exists.
			fs.delete(outputPath,true);
			fs.close();

			// Create job
			job = new Job(conf, "MatrixNorm");
			job.setJarByClass(NormMatrix.class);

			// Specify key / value
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);

			// Setup MapReduce job
			job.setMapperClass(Map.class);
			job.setCombinerClass(Combine.class);
			job.setReducerClass(Reduce.class);

			// Set only the number of reduces tasks
			job.setNumReduceTasks(1);

			// Set Map output Key/Value type 
			job.setMapOutputKeyClass(LongWritable.class);
			job.setMapOutputValueClass(DoubleWritable.class);

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
