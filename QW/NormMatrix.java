package mult;

import java.io.IOException;
import java.net.URI;

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
 
public class NormMatrix {
 
	public static class Map extends Mapper<LongWritable, Text, LongWritable, DoubleWritable> {
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		    
			String line = value.toString();
			String[] records = line.split(","); // "," is the delimiter used in the input file.
			DoubleWritable outputReal = new DoubleWritable();
			DoubleWritable outputImaginary = new DoubleWritable();
			double[] element = new double[2];
			String[] vals;

			if (records[0].indexOf("#") == -1) { // # is the header of the matrix file.

				vals = records[3].split("j");
				element[0] = Double.parseDouble(vals[0]);
				element[1] = Double.parseDouble(vals[1]);
				outputReal.set(element[0]*element[0]);
				outputImaginary.set(element[1]*element[1]);

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

			if (key.toString().equals("0")) { // 0 is the real part of the number

				for (DoubleWritable val : values) {
					sum += val.get();
				}

				context.write(null, new Text("Real=" + Double.toString(Math.sqrt(sum))));	
			
			} else {

				if (key.toString().equals("1")) { // 1 is the imaginary part of the number

					for (DoubleWritable val : values) {
						sum += val.get();
					}

					context.write(null, new Text("Imaginary=" + Double.toString(Math.sqrt(sum))));		
			
				}

			}

		}
	}
	
 
	public static void main(String[] args) throws Exception {

		try{

			Configuration conf = new Configuration();

			Path inputPath = new Path(args[0]);
			Path outputPath = new Path(args[1]);

			FileSystem  fs = FileSystem.get(new URI(outputPath.toString()), conf);		

			// Delete the output directory if it already exists.
			fs.delete(outputPath,true);
			fs.close();

			// Create job
			Job job = new Job(conf, "MatrixNorm");
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
