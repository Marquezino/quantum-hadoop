/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package operations;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


/**
 * This software calculate the square of the absolute value of each element of
 * a complex array using Apache Hadoop.
 *
 * @version 1.0 7 Jun 2015
 * @author David Souza
 */


public class AbsSquare {

    /*
     * Type of the array: A or B.
     */
    private static final int TYPE = 0;

    /*
     * Index of the array.
     */
    private static final int INDEX = 1;

    /*
     * Index of the array.
     */
    private static final int EXTRA_INDEX = 2;

    /*
     * Value of a index of the array.
     */
    private static final int VALUE = 3;


    public static class Map extends
            Mapper<LongWritable, Text, LongWritable, Text> {
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString();
            // "," is the delimiter used in the input file.
            String[] records = line.split(",");
            String[] vals;
            double real;
            double imaginary;
            String fullOutput = records[TYPE] + "," + records[INDEX] + ","
                                    + records[EXTRA_INDEX] + ",";

            // # is the header of the matrix file.
            if (records[0].indexOf("#") == -1) {

                vals = records[VALUE].split("j");
                
                real = Double.parseDouble(vals[0]);
                imaginary = Double.parseDouble(vals[1]);
                fullOutput += Double.toString(Math.pow(Math.sqrt((real * real
                                + imaginary * imaginary)), 2));

                context.write(key, new Text(fullOutput));

            } else {

                context.write(new LongWritable(-1), new Text(records[TYPE] + ","
                        + records[INDEX] + "," + records[EXTRA_INDEX]));
            }
        }

    }


    public static class Reduce extends Reducer<LongWritable, Text,
            LongWritable, Text> {
        public void reduce(LongWritable key, Iterable<Text> values,
                Context context) throws IOException, InterruptedException {

            for (Text val : values) {
                    context.write(null, val);
            }

        }
    }


    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Path inputPath;
        Path outputPath;
        FileSystem  fs;
        Job job;

        try {

            inputPath = new Path(args[0]);
            outputPath = new Path(args[1]);

            /*
             * Disable the map output compression in Hadoop for gain of
             * performance
             */
            conf.set("mapred.compress.map.output", "false");

            // Set the key/value separator
            conf.set("mapred.textoutputformat.separator", ",");

            fs = FileSystem.get(new URI(outputPath.toString()), conf);

            // Delete the output directory if it already exists.
            fs.delete(outputPath, true);
            fs.close();

            // Create job
            job = new Job(conf, "AbsSquare");
            job.setJarByClass(AbsSquare.class);

            // Specify key / value
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            // Setup MapReduce job
            job.setMapperClass(Map.class);
            job.setReducerClass(Reduce.class);

            // Set only the number of reduces tasks
            job.setNumReduceTasks(1);

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


        } catch (Exception e) {
            System.out.println(e);
        }

    }
}
