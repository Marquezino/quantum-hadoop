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
import java.io.BufferedReader;
import java.io.InputStreamReader;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
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
 * This software give a new shape to a one dimensional array using Apache
 * Hadoop.
 *
 * @version 1.0 12 Jun 2015
 * @author David Souza
 */


public class Reshape {

    /*
     * Length of the array generated with the line split of a input file.
     */
    private static final int ARRAY_LENGTH = 4;

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

            Configuration conf = context.getConfiguration();
            // The new shape of the array
            String[] input = conf.get("input").split(",");
            String fullInput = conf.get("input");
            String line = value.toString();
            // "," is the delimiter used in the input file.
            String[] records = line.split(",");
            int[] output = new int[input.length];
            int total = 1;
            int idx = Integer.parseInt(records[INDEX]);
            String fullOutput = records[TYPE] + ",";

            // # is the header of the matrix file.
            if (records[TYPE].indexOf("#") == -1) {

                for (int i = 0; i < input.length; i++) {

                    for (int j = i + 1; j < input.length; j++) {

                        total *= Integer.parseInt(input[j]);
                    }

                    output[i] = (int) (idx / total);
                    idx -= output[i] * total;
                    total = 1;

                }

                for (int i = 0; i < output.length; i++) {

                    fullOutput += Integer.toString(output[i]) + ",";
                }

                fullOutput += records[VALUE];

                context.write(key, new Text(fullOutput));

            } else {

                context.write(key , new Text(records[TYPE] + "," + fullInput));
            }
        }

    }


    public static class Reduce extends Reducer<LongWritable, Text,
            Text, Text> {
        public void reduce(LongWritable key, Iterable<Text> values,
                Context context) throws IOException, InterruptedException {

            for (Text val : values) {
                context.write(null, val);
            }

        }
    }


    public static void verifyFormat(FileSystem fs, Path inputPath,
            String input) {

        BufferedReader br;
        FileStatus[] status;
        String line;
        String matrixType = "";
        String[] vals;
        String[] newShape;
        int[] valueShape;
        int numberShape = 1;
        boolean header = false;

        try {

            status = fs.listStatus(inputPath);
            for (int i = 0; i < status.length; i++) {

                if (status[i].getPath().toString().indexOf("_logs") > -1) {
                    continue;
                } else {
                    if (status[i].getPath().toString().indexOf("_SUCCESS") > -1) {
                        continue;
                    }
                }

                br = new BufferedReader(new InputStreamReader(fs.open(status[i].
                        getPath())));

                line = br.readLine();

                if (line == null) {
                    continue;
                }

                if (line.indexOf("#A") > -1) {

                    header = true;
                    vals = line.split(",");
                    newShape = input.split(",");
                    valueShape = new int[newShape.length];

                    for (int j = 0; j < newShape.length; j++) {

                        valueShape[j] = Integer.parseInt(newShape[j]);
                    }

                    for (int j = 0; j < valueShape.length; j++) {

                        numberShape *= valueShape[j];
                    }

                    if (numberShape != Integer.parseInt(vals[1])) {

                        System.out.println("The shape given in the input does "
                                + "not fit with the dimension in the input file"
                                + ".");
                        System.exit(1);
                    }

                    if (matrixType == "") {

                        matrixType = "A";
                    }
                    
                    line = br.readLine();

                } else {

                    if (line.indexOf("#B") > -1) {

                        header = true;
                        vals = line.split(",");
                        newShape = input.split(",");
                        valueShape = new int[newShape.length];

                        for (int j = 0; j < newShape.length; j++) {

                            valueShape[j] = Integer.parseInt(newShape[j]);
                        }

                        for (int j = 0; j < valueShape.length; j++) {

                            numberShape *= valueShape[j];
                        }

                        if (numberShape != Integer.parseInt(vals[1])) {

                            System.out.println("The shape given in the input "
                                    + "does not fit with the dimension in the "
                                    + "input file.");
                            System.exit(1);
                        }

                        if (matrixType == "") {

                            matrixType = "B";
                        }

                        line = br.readLine();                         
                    }                   
                }

                if (line == null) {
                    continue;
                }

                vals = line.split(",");

                if (matrixType == "") {

                    if ((vals[0] == "A") || (vals[0] == "B")) {

                        matrixType = vals[0];
                    } else {

                        matrixType = "";
                    }
                }

                if ((vals.length != ARRAY_LENGTH)
                        || !vals[0].equals(matrixType)) {

                    System.out.println("======================================="
                            + "==============\nThe format of the input array is"
                            + " not supported.\nPut it in this format:\n\t"
                            + "matrixType,index,extraIndex,value\nWith:\n"
                            + "\tmatrixType = A or B\n\tindex = integer number"
                            + "\n\textraIndex = 0 (integer zero)\n\tvalue = "
                            + "any number type\n\nThe file should have a header"
                            + ":\n\t#matrixType,index,extraIndex\n"
                            + "With:\n\tmatrixType = A or B\n\tindex = integer "
                            + "number\n\textraIndex = 1 (integer one)\n========"
                            + "=============================================");

                    System.exit(1);
                }                

                br.close();
            }

            if (matrixType == "") {

                System.out.println("The input has no data files.");
                System.exit(1);
            }

            if (!header) {

                System.out.println("The input has no header.");
                System.exit(1);
            }

        } catch (Exception e) {
            System.out.println(e);
        }
    }


    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Path inputPath;
        Path outputPath;
        FileSystem  fs;
        Job job;


        try {

            inputPath = new Path(args[1]);
            outputPath = new Path(args[2]);

            fs = FileSystem.get(new URI(outputPath.toString()), conf);

            /*
             * Disable the map output compression in Hadoop for gain of
             * performance
             */
            conf.set("mapred.compress.map.output", "false");

            // Set the key/value separator
            conf.set("mapred.textoutputformat.separator", ",");

            // The new shape of the array
            conf.set("input", args[0]);

            verifyFormat(fs, inputPath, args[0]);

            // Delete the output directory if it already exists.
            fs.delete(outputPath, true);
            fs.close();

            // Create job
            job = new Job(conf, "Reshape");
            job.setJarByClass(Reshape.class);

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
