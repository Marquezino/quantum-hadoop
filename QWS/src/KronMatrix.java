/*
 * KronMatrix.java	1.2 2015/04/23
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
import java.util.ArrayList;
import java.net.URI;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;

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


/**
 *
	This software calculate the Kronecker product between two matrices
	using Apache Hadoop.
 *
 * @version
	1.2 23 Apr 2015  * @author
	David Souza  */


public class KronMatrix {

	/* Kronecker Index Equation:
	 *
	 * Kronecker row: matrixA (tensor) matrixB =
	 *                                          number_rows(matrixB)*(row_matrixA - 1) + row_matrixB; if the index start with 1
	 *                                          number_rows(matrixB)*(row_matrixA) + row_matrixB; if the index start with 0
	 *
	 * Kronecker column: matrixA (tensor) matrixB =
	 *                                          number_columns(matrixB)*(column_matrixA - 1) + column_matrixB; if the index start with 1
	 *                                          number_columns(matrixB)*(column_matrixA) + column_matrixB; if the index start with 0
	 */

	private static final long NUMBER_ELEMENTS_IN_MEMORY = 1000000; // Number of elements that will be loaded in the RAM memory in reduce function.
	
	public static class Map extends Mapper<LongWritable, Text, Text, Text> {
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		    
			String line = value.toString();
			String[] records = line.split(",");	// "," is the delimiter used in the input file.
			Text outputKey = new Text();
			Text outputValue = new Text();
			Configuration conf = context.getConfiguration();
			long number_elements_A = Long.parseLong(conf.get("number_elements_A"));	// Number of elements of the matrix A
			long number_elements_B = Long.parseLong(conf.get("number_elements_B"));	// Number of elements of the matrix B
			long number_of_parts = Long.parseLong(conf.get("number_of_parts"));	// Number of parts that the matrix A was splitted
			long partitionA;


			if (number_elements_B <= NUMBER_ELEMENTS_IN_MEMORY) {

				if (records[0].equals("A")) {	// A is the left matrix.

					outputKey.set(records[1]);
					outputValue.set("A," + records[2] + "," + records[3] + "," + records[4]);
					context.write(outputKey, outputValue);

				} else {

					if (records[0].equals("B")) {	// B is the right matrix.

						outputKey.set(records[1]);
						outputValue.set("B," + records[2] + "," + records[3] + "," +
							records[4]);
						context.write(outputKey, outputValue);

					} else {
						if ((records[0].equals("#A")) || (records[0].equals("#B"))) {	// #A and #B are the header with the matrix dimension.
							
							outputKey.set("-1");	// This key represents the header of the files (both matrices dimensions)
							outputValue.set(records[1] + "," + records[2]);
							context.write(outputKey, outputValue);
						}
					}
				}
			} else {
				
				if (records[0].equals("A")) {	// A is the left matrix.

					outputKey.set(records[1]);
					outputValue.set("A," + records[2] + "," + records[3] + "," + records[4]);
					context.write(outputKey, outputValue);

				} else {

					if (records[0].equals("B")) {	// B is the right matrix.

						partitionA = Long.parseLong(records[1]) % number_of_parts;

						outputKey.set(Long.toString(partitionA) + "_" + records[1]);
						outputValue.set("B," + records[2] + "," + records[3] + "," +
							records[4]);
						context.write(outputKey, outputValue);

					} else {
						if ((records[0].equals("#A")) || (records[0].equals("#B"))) {	// #A and #B are the header with the matrix dimension.
						
							outputKey.set("-1");	// This key represents the header of the files (both matrices dimensions)
							outputValue.set(records[1] + "," + records[2]);
							context.write(outputKey, outputValue);
						}
					}
				}
				

			}
			
		}
	}
 
	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			
			String[] value;
			String[] valA = new String[2];
			String[] valB = new String[2];
			String[] coordinatesA = new String[3];
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
			String keyA="";
			String partA="";
			long number_of_parts = (total_number_elements_B / NUMBER_ELEMENTS_IN_MEMORY);
			long rows=1;
			long columns=1;

			
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

				if ((valA[0] == null) && (valA[1] == null)){
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
							Double.toString(Double.parseDouble(valA[0]) * Double.parseDouble(valB[0])) + "j" +
							Double.toString(Double.parseDouble(valA[1]) * Double.parseDouble(valB[1])) );

						context.write(null, output);

					}
					
				}

			} else {
				
				for (Text val : values) {
					value = val.toString().split(",");
					rows *= Long.parseLong(value[0]);
					columns *= Long.parseLong(value[1]);
				}
				
				context.write(null, new Text("#" + type_matrix_output + "," + Long.toString(rows) + "," +
					Long.toString(columns)));	
			}

		}
	}
	
 
	public static void main(String[] args) throws Exception {

		long countA = 0;
		BufferedReader br;
		String old_path;
		String new_path;
		String line;
		String first_line;
		boolean new_size;
		boolean verified;
		String[] val;
		String[] temp_val;
		String temp;
		BufferedWriter bw;
		long countB = 0;
		boolean cf_countB = false;
		long number_of_parts = 1;

		Configuration conf = new Configuration();
		Path inputPath;
		Path outputPath;
		FileSystem fs_input;
		FileStatus[] status;
		FileSystem  fs;


		try{
			inputPath = new Path(args[0]);
			outputPath = new Path(args[1]);

			conf.set("type_matrix_output", args[2]);	// Set if the output will be matrix type A ou type B

			fs_input = FileSystem.get(conf);
			status = fs_input.listStatus(inputPath);	// The names of all files in the input path
			
			for (int i = 0; i < status.length; i++){
				br = new BufferedReader(new InputStreamReader(fs_input.open(status[i].getPath())));
				old_path = status[i].getPath().toString();
				new_path = old_path+"_new";
				first_line="splitted";
				new_size = false;
				verified = false;
				line=br.readLine();

				// Empty file. Go to the next.
				if (line == null){
					continue;
				}
				
				if (line.indexOf("A") > -1) {

					bw = new BufferedWriter(new OutputStreamWriter(
						fs_input.create(new Path(new_path),true)));

					while (line != null){
						if (!(line.equals("")) && !(String.valueOf(line.charAt(0)).equals(" "))){

							val = line.split(",",3);
							if (!val[0].equals("#A")) {
					
								if (!new_size && !verified) {
									temp_val = line.split(",");
									verified = true;
									if (temp_val.length < 5) {
										new_size = true;
									}
								}

								if (new_size) {
									val[0] = "A," + Long.toString(countA) + ",";
									if (first_line.equals("")) {
										bw.write("\n"+val[0]+val[1]+","+val[2]);
									} else {
										bw.write(val[0]+val[1]+","+val[2]);
										first_line = "";
									}
								}
								
							} else {
								bw.write(line);
								first_line = "";
								countA--;
							}

							if (new_size || val[1].indexOf("_") == -1) {
								
								countA++;

							} else {
								temp_val = val[1].split("_");
								if (countA < Long.parseLong(temp_val[1])) {
									countA = Long.parseLong(temp_val[1]) + 1;
								} 
							}
						}
					
						line=br.readLine();
						
					}
					
					if (new_size) {
						bw.close();
						System.out.println("New format to matrix A. Old format deleted.");
						// Delete the old file that haven't the count.
						fs_input.delete(status[i].getPath(),true);
					} else {
						bw.close();
						fs_input.delete(new Path(new_path),true);
					}

				}

				br.close();
				
			}

			status = fs_input.listStatus(inputPath);
			for (int i = 0; i < status.length; i++){
				br = new BufferedReader(new InputStreamReader(fs_input.open(status[i].getPath())));
				old_path = status[i].getPath().toString();
				new_path = old_path+"_new";
				first_line="splitted";
				new_size = false;
				verified = false;
				line=br.readLine();

				// Empty file. Go to the next.
				if (line == null){
					continue;
				}

				if (line.indexOf("#B") > -1) {
					String[] vals = line.split(",");
					conf.set("number_rows_B", vals[1]);
					conf.set("number_columns_B", vals[2]);
				}
				
				if (line.indexOf("B") > -1) {
												
					bw = new BufferedWriter(new OutputStreamWriter(
						fs_input.create(new Path(new_path),true)));

					while (line != null){
						if (!(line.equals("")) && !(String.valueOf(line.charAt(0)).equals(" "))){

							val = line.split(",",2);
							if (!val[0].equals("#B")) {
				
								if (!new_size && !verified) {
									temp_val = line.split(",");
									verified = true;
									if (temp_val.length < 5) {
										new_size = true;
									}
								}

								if (new_size) {
									if (first_line.equals("")) {
										for (long j=0; j < countA; j++) {
											val[0] = "B," + Long.toString(j) + ",";
											bw.write("\n" + val[0] + val[1]);
										}
										
									} else {
										val[0] = "B,0,";
										bw.write(val[0]+val[1]);
										for (long j=1; j < countA; j++) {
											val[0] = "B," + Long.toString(j) + ",";
											bw.write("\n" + val[0] + val[1]);
										}
										first_line = "";
									}
								} else {
									cf_countB = true;
								}
							
							} else {
								bw.write(line);
								first_line = "";
								countB--;
							}

							countB++;
						}
				
						line=br.readLine();
					
					}
				
					if (new_size) {
						bw.close();
						System.out.println("New format to matrix B. Old format deleted.");
						// Delete the old file that haven't the count.
						fs_input.delete(status[i].getPath(), true);
					} else {
						bw.close();
						fs_input.delete(new Path(new_path), true);
					}

				}

				br.close();
				
			}

			if (cf_countB) {
				countB /= countA;
			}

			status = fs_input.listStatus(inputPath);
			if (countB > NUMBER_ELEMENTS_IN_MEMORY) {

				if (countB % NUMBER_ELEMENTS_IN_MEMORY == 0) {
					number_of_parts = (countB / NUMBER_ELEMENTS_IN_MEMORY);
				} else {
					number_of_parts = (countB / NUMBER_ELEMENTS_IN_MEMORY) + 1;
				}
			
				for (int i = 0; i < status.length; i++){
					br = new BufferedReader(new InputStreamReader(fs_input.open(status[i].getPath())));
					old_path = status[i].getPath().toString();
					new_path = old_path+"_parts";
					first_line="splitted";
					new_size = false;
					verified = false;
					line=br.readLine();

					// Empty file. Go to the next.
					if (line == null){
						continue;
					}
				
					if (line.indexOf("A") > -1) {

						bw = new BufferedWriter(new OutputStreamWriter(
							fs_input.create(new Path(new_path), true)));

						while (line != null){
							if (!(line.equals("")) && !(String.valueOf(line.charAt(0)).equals(" "))){

								val = line.split(",",3);
								if (!val[0].equals("#A")) {
					
									if (!new_size && !verified) {
										temp_val = line.split(",");
										verified = true;
										if (temp_val.length == 5 && temp_val[1].indexOf("_") == -1) {
											new_size = true;
										}
									}

									if (new_size) {
										if (first_line.equals("")) {
											for (long j=0; j < number_of_parts; j++) {
												temp = "," + Long.toString(j) + "_" + val[1] + ",";
												bw.write("\n"+val[0]+temp+val[2]);
											}
										} else {
											temp = ",0_" + val[1] + ",";
											bw.write(val[0]+temp+val[2]);
											for (long j=1; j < number_of_parts; j++) {
												temp = "," + Long.toString(j) + "_" + val[1] + ",";
												bw.write("\n"+val[0]+temp+val[2]);
											}
											first_line = "";
										}

									}
								
								} else {
									bw.write(line);
									first_line = "";
								}

							}
							if (new_size) {
								line=br.readLine();
							} else {
								line=null;
							}
							
						
						}
					
						if (new_size) {
							bw.close();
							System.out.println("New format to matrix A: with parts. Old format deleted.");
							// Delete the old file that haven't the count.
							fs_input.delete(status[i].getPath(), true);
						} else {
							bw.close();
							fs_input.delete(new Path(new_path), true);
						}

					}

					br.close();
				
				}

			}

			fs_input.close();

			//System.out.println("number_elements_A="+Long.toString(countA));
			//System.out.println("number_elements_B="+Long.toString(countB));
			//System.out.println("number_of_parts="+Long.toString(number_of_parts));

			// Set the number of elements of the matrix A and matrix B that are stored in the input files
			conf.set("number_elements_A", Long.toString(countA));
			conf.set("number_elements_B", Long.toString(countB));
			
			// Set the number of parts that the matrix A was splitted
			conf.set("number_of_parts", Long.toString(number_of_parts));
			

			fs = FileSystem.get(new URI(outputPath.toString()), conf);		

			// Delete the output directory if it already exists.
			fs.delete(outputPath, true);
			fs.close();

			// Create job
			Job job = new Job(conf, "KroneckerProduct");
			job.setJarByClass(KronMatrix.class);

			// Specify key / value
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);

			// Setup MapReduce job
			job.setMapperClass(Map.class);
			job.setReducerClass(Reduce.class);

			// Set only the number of reduces tasks
			//job.setNumReduceTasks(Integer.parseInt(args[3]));

			// Set Map output Key/Value type 
			job.setMapOutputKeyClass(Text.class);
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
