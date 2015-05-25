package mult;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.net.URI;
import java.io.BufferedReader;
import java.io.InputStreamReader;

// Hadoop libraries -------------------------------------------------------------------------------------
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
// End of Hadoop libraries ---------------------------------------------------------------------------------
 
public class MultMatrix {
 
	public static class Map_Multiplication extends Mapper<LongWritable, Text, Text, Text> {
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		    
			String line = value.toString();
			String[] records = line.split(","); // "," is the delimiter used in the input file.
			Text outputKey = new Text();
			Text outputValue = new Text();

			if (records[0].equals("A")) { // A is the left matrix.

				outputKey.set(records[2]);
				outputValue.set("A," + records[1] + "," + records[2] + "," + records[3]);
				context.write(outputKey, outputValue);

			} else {
				if (records[0].equals("B")) { // B is the right matrix.

					outputKey.set(records[1]);
					outputValue.set("B," + records[1] + "," + records[2] + "," + records[3]);
					context.write(outputKey, outputValue);
				}
				else {
					if (records[0].indexOf("#") > -1) { // The file line started with # is the header with the matrices dimensions.

						outputKey.set("#");
						outputValue.set(records[0] + "," + records[1] + "," + records[2]);
						context.write(outputKey, outputValue);
					}
				}
			}
		}
	}

	public static class Map_Dot extends Mapper<LongWritable, Text, Text, Text> {
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		    
			String line = value.toString();
			String[] records = line.split(","); // "," is the delimiter used in the input file.
			Text outputKey = new Text();
			Text outputValue = new Text();

			if (records[0].equals("A")) { // A is the left matrix.
				
				outputKey.set(records[2]);
				outputValue.set("A," + records[1] + "," + records[2] + "," + records[3]);
				context.write(outputKey, outputValue);

			} else {
				if (records[0].equals("B")) { // B is the right matrix.

					outputKey.set(records[2]);
					outputValue.set("B," + records[1] + "," + records[2] + "," + records[3]);
					context.write(outputKey, outputValue);
				}
				else {
					if (records[0].indexOf("#") > -1) { // The file line started with # is the header with the matrices dimensions.

						outputKey.set("#");
						outputValue.set(records[0] + "," + records[1] + "," + records[2]);
						context.write(outputKey, outputValue);
					}
				}
			}
		}
	}
 
	public static class Reduce_Multiplication extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			
			String[] value, valA, valB;
			String rows="", columns="";
			List<String[]> listA = new ArrayList<String[]>();
			List<String[]> listB = new ArrayList<String[]>();
			Text output = new Text();	

			for (Text val : values) {
				value = val.toString().split(",");
				if (value[0].equals("A")) {
					listA.add(new String[]{value[1],value[2],value[3]});

				} else {
					if (value[0].equals("B")) {
						listB.add(new String[]{value[1],value[2],value[3]});

					} else { // Write the matrix dimension in the file.
						
						if (value[0].equals("#A")) {
							rows = value[1];
						}

						if (value[0].equals("#B")) {
							columns = value[2];
						}
					}
				}
			}

			if (!rows.equals("") && !columns.equals("")) {
				context.write(null, new Text(key.toString() + ";" + rows + "," + columns));
			}
			
			for (String[] elementA : listA) {
				for (String[] elementB : listB) {

					valA = elementA[2].split("j");
					valB = elementB[2].split("j");

					output.set( elementA[0] + "," + elementB[1] + ";" +  
							Double.toString(Double.parseDouble(valA[0])*Double.parseDouble(valB[0])) + "j" +
							Double.toString(Double.parseDouble(valA[1])*Double.parseDouble(valB[1])) );

					context.write(null, output);
				}
			}

		}
	}

	
	public static class Reduce_Dot extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			
			String[] value, valA, valB;
			String rows="", columns="";
			List<String[]> listA = new ArrayList<String[]>();
			List<String[]> listB = new ArrayList<String[]>();
			Text output = new Text();		

			for (Text val : values) {
				value = val.toString().split(",");
				if (value[0].equals("A")) {
					listA.add(new String[]{value[1],value[2],value[3]});

				} else {
					if (value[0].equals("B")) {
						listB.add(new String[]{value[1],value[2],value[3]});

					} else { // Write the matrix dimension in the file.
						
						if (value[0].equals("#A")) {
							columns = value[1];
						}

						if (value[0].equals("#B")) {
							rows = value[1];
						}
					}
				}
			}

			if (!rows.equals("") && !columns.equals("")) {
				context.write(null, new Text(key.toString() + ";" + rows + "," + columns));
			}
			
			for (String[] elementA : listA) {
				for (String[] elementB : listB) {
					valA = elementA[2].split("j");
					valB = elementB[2].split("j");

					output.set( elementB[0] + "," + elementA[0] + ";" +  
							Double.toString(Double.parseDouble(valA[0])*Double.parseDouble(valB[0])) + "j" +
							Double.toString(Double.parseDouble(valA[1])*Double.parseDouble(valB[1])) );

					context.write(null, output);
				}
			}

		}
	}


	public static class Map_Result extends Mapper<LongWritable, Text, Text, Text> {

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			String[] vals = value.toString().split(";");
	
			context.write(new Text(vals[0]), new Text(vals[1]));

		}
	}


	public static class Combine_Result extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			String[] value;
			double real=0.0d, imaginary=0.0d;

			if (!key.toString().equals("#")) {
	
				for (Text val : values){					
					value = val.toString().split("j");
					real += Double.parseDouble(value[0]);
					imaginary += Double.parseDouble(value[1]);
				}

				if (real != 0.0d || imaginary != 0.0d) {

					context.write(key, new Text(Double.toString(real)+"j"+Double.toString(imaginary)));

				}

			} else {
				for (Text val : values) {
					context.write(key, val);
				}
			}

		
		}
	}


	public static class Reduce_Result extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			
			String[] value;
			double real=0.0d, imaginary=0.0d;
			Configuration conf = context.getConfiguration();
			String type_matrix_output = conf.get("type_matrix_output");	

			if (!key.toString().equals("#")) {
	
				for (Text val : values){					
					value = val.toString().split("j");
					real += Double.parseDouble(value[0]);
					imaginary += Double.parseDouble(value[1]);
				}

				if (real != 0.0d || imaginary != 0.0d) {

					context.write(null, new Text(type_matrix_output + "," + key.toString() + "," + 
								Double.toString(real) + "j" + Double.toString(imaginary)));
				}

			} else {
				for (Text val : values) {
					value = val.toString().split(",");
					context.write(null, new Text("#" + type_matrix_output + "," + value[0] + "," + value[1])); // Write the matrix dimension in the file.
				}
			}

		}
	}

 
	public static void main(String[] args) throws Exception {

		try{

			boolean dot_product = false;
			String[] dim_A = new String[2], dim_B = new String[2];

			Configuration conf = new Configuration();

			conf.set("type_matrix_output",args[3]); // Set if the output will be matrix type A ou type B

			Path inputPath = new Path(args[0]);
			Path outputPath = new Path(args[1]);

			FileSystem fs_input = FileSystem.get(conf);
			FileStatus[] status = fs_input.listStatus(inputPath); // The names of all files in the input path

			for (int i = 0; i < status.length; i++) {
				BufferedReader br = new BufferedReader(new InputStreamReader(fs_input.open(status[i].getPath())));
				String line = br.readLine();

				// Empty file. Go to the next.
				if (line == null){
					br.close();
					continue;
				}

				if (line.indexOf("#A") > -1) {
					String[] vals = line.split(",");
					dim_A[0] = vals[1];
					dim_A[1] = vals[2];
					
				} else {
					if (line.indexOf("#B") > -1) {
						String[] vals = line.split(",");
						dim_B[0] = vals[1];
						dim_B[1] = vals[2];
					}

				}
				br.close();
			}

			fs_input.close();

			if (!dim_A[1].equals(dim_B[0])) { // A_(m x n) and B_(p x q) -> verify if n==p, constraint for matrix multiplication
				
				if (dim_B[0].equals("1") && dim_A[1].equals(dim_B[1])) {
					dot_product = true;
					System.out.print("\n----------------------------------------------------------------\n");
					System.out.println("You can not perform the multiplication between the matrices in the input path. The constraint below should be satisfied:\nThe number of columns in the matrix A should be equal to the number of rows in the matrix B.\nPerforming dot product in instead.");
					System.out.println("----------------------------------------------------------------\n");
				} else {
					System.out.print("\n----------------------------------------------------------------\n");
					System.out.println("You can not perform the multiplication or the dot product between the matrices in the input path. One of these three constraints did not be satisfied:\n1- The number of columns in the matrix A should be equal to the number of rows in the matrix B.\n2- If the number of rows in the matrix B is equal to 1 then the number of columns in matrix A should be equal the number of columns in matrix B.\n3- If the number of columns in the matrix A is not equal to the number of rows in the matrix B then the number of rows in the matrix B should be equal to 1.");
					System.out.println("----------------------------------------------------------------\n");
					System.exit(1);
				}

			} else {
				System.out.print("\n----------------------------------------------------------------\n");
				System.out.println("The dot product between two matrices is equals to the multiplication between them.");
				System.out.println("----------------------------------------------------------------\n");
			}



			FileSystem  fs = FileSystem.get(new URI(outputPath.toString()), conf);

			// Delete the output directory if it already exists.
			fs.delete(outputPath,true);
			fs.close();

			// Create job
			Job job = new Job(conf, "MatrixMultiplicationStep1");
			job.setJarByClass(MultMatrix.class);

			// Specify key / value
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);

			if (dot_product) {
				// Setup MapReduce job
				job.setMapperClass(Map_Dot.class);
				job.setReducerClass(Reduce_Dot.class);
			} else {
				// Setup MapReduce job
				job.setMapperClass(Map_Multiplication.class);
				job.setReducerClass(Reduce_Multiplication.class);
			}

			// Set only the number of reduces tasks
			//job.setNumReduceTasks(Integer.parseInt(args[4]));

			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);

			// Input
			FileInputFormat.addInputPath(job, inputPath);

			// Output
			FileOutputFormat.setOutputPath(job, outputPath);

			// Execute job
			job.waitForCompletion(true);

			// The second Map/Reduce job
			Path inputPath2 = new Path(args[1]);
			Path outputPath2 = new Path(args[2]);
			FileSystem  fs2 = FileSystem.get(new URI(outputPath2.toString()), conf);

			// Delete the output directory if it already exists.
			fs2.delete(outputPath2,true);

			// Create job
			Job job2 = new Job(conf, "MatrixMultiplicationStep2");
			job2.setJarByClass(MultMatrix.class);

			// Specify key / value
			job2.setOutputKeyClass(Text.class);
			job2.setOutputValueClass(Text.class);

			// Setup MapReduce job
			job2.setMapperClass(Map_Result.class);
			job2.setCombinerClass(Combine_Result.class);
			job2.setReducerClass(Reduce_Result.class);

			// Set only the number of reduces tasks
			//job2.setNumReduceTasks(Integer.parseInt(args[4]));

			job2.setInputFormatClass(TextInputFormat.class);
			job2.setOutputFormatClass(TextOutputFormat.class);

			// Input
			FileInputFormat.addInputPath(job2, inputPath2);

			// Output
			FileOutputFormat.setOutputPath(job2, outputPath2);

			// Execute job
			job2.waitForCompletion(true);
		
			// Delete the temporary directory after job execution.
			fs2.delete(inputPath2,true);
			fs2.close();
		
		} catch(Exception e){
			System.out.println(e);
		}

	}
}
