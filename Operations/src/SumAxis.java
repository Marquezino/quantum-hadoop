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
import java.util.Arrays;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileUtil;
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
 * This software calculate the sum of array over a set of axes using Apache
 * Hadoop.
 *
 * @version 1.0 13 Jun 2015
 * @author David Souza
 */


public class SumAxis {

    /*
     * Length of the array generated with the line split of a input file.
     */
    private static final int ARGS_LENGTH = 4;

    /*
     * The amount of values that are not indexes.
     */
    private static final int NOT_INDEX = 2;

    public static class Map extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException,
                IllegalArgumentException {

            Configuration conf = context.getConfiguration();
            String line = value.toString();
            String[] records = line.split(",");
            int axis = Integer.parseInt(conf.get("axis"));
            String fullFixedAxis = conf.get("fixedAxis");
            String[] fixedAxis;
            String outputKey = records[0];
            String outputValue = "";
            boolean writeOutput = true;

            // # is the header of the matrix file.
            if (records[0].indexOf("#") == -1) {

                if (axis == records.length - 1) {
                    throw new IllegalArgumentException("The axis should be "
                            + "small than " + Integer.toString(
                            records.length - 1) + ".");
                }

                if (fullFixedAxis.equals("empty")) {

                    for (int i = 1; i < records.length - 1; i++) {

                        if (i != axis) {

                            outputKey += "," + records[i];
                        }
                    }

                    outputValue += records[records.length - 1];

                    context.write(new Text(outputKey) , new Text(outputValue));

                } else {
                    fixedAxis = fullFixedAxis.split(",");
                    if (fixedAxis.length == records.length - NOT_INDEX) {

                        for (int i = 0; i < fixedAxis.length; i++) {
                            if (fixedAxis[i].equals("?")) {
                                if (i + 1 != axis) {
                                    outputKey += "," + records[i+1];
                                }

                            } else {
                                if (fixedAxis[i].equals(records[i+1])) {
                                    //if (i + 1 != axis) {
                                        continue;
                                        //outputKey += "," + records[i+1];
                                    //}

                                } else {
                                    writeOutput = false;
                                    break;
                                }
                            }
                        }
                        outputValue += records[records.length - 1];
                        if (writeOutput) {
                            context.write(new Text(outputKey) , new Text(
                                    outputValue));
                        }
                    } else {
                        throw new IllegalArgumentException("You should give "
                                + Integer.toString(records.length - NOT_INDEX)
                                + "values to be fixed. Use the character ? in "
                                + "a not fixed value.");
                    }
                }

            } else {

                if (axis == records.length) {
                    throw new IllegalArgumentException("The axis should be "
                            + "small than " + Integer.toString(records.length)
                            + ".");
                }

                if (fullFixedAxis.equals("empty")) {

                    for (int i = 1; i < records.length; i++) {

                        if (i != axis) {

                            outputValue += records[i];
                            if (i + 1 != records.length) {
                                outputValue += ",";
                            }
                        }
                    }

                    context.write(new Text(outputKey) , new Text(outputValue));

                } else {
                    fixedAxis = fullFixedAxis.split(",");
                    if (fixedAxis.length == records.length - 1) {

                        for (int i = 0; i < fixedAxis.length; i++) {
                            if (fixedAxis[i].equals("?")) {
                                if (i + 1 != axis) {
                                    if (!outputValue.equals("")) {
                                        outputValue += ",";
                                    }
                                    outputValue += records[i+1];
                                }

                            } //else {
                                //if (fixedAxis[i].equals(records[i+1])) {
                                 //   if (i != axis) {
                                  //      outputValue += records[i+1];
                                   //     if (i + 1 != fixedAxis.length) {
                                     //       outputValue += ",";
                                       // }
                                    //}

                                //} else {
                                //    writeOutput = false;
                                //    break;
                                //}
                            //}
                        }
                        //if (writeOutput) {
                            context.write(new Text(outputKey) , new Text(
                                    outputValue));
                        //}
                    } else {
                        throw new IllegalArgumentException("You should give "
                                + Integer.toString(records.length - NOT_INDEX)
                                + "values to be fixed. Use the character ? in "
                                + "a not fixed value.");
                    }
                }
            }
        }

    }


    public static class Combine extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            double sum = 0.0d;

            if (key.toString().indexOf("#") == -1) {

                for (Text val : values) {
                    sum += Double.parseDouble(val.toString());
                }

                context.write(key, new Text(Double.toString(sum)));

            } else {
                for (Text val : values) {
                    context.write(key, val);
                }
            }

        }
    }


    public static class Reduce extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            double sum = 0.0d;

            if (key.toString().indexOf("#") == -1) {

                for (Text val : values) {
                    sum += Double.parseDouble(val.toString());
                }

                context.write(key, new Text(Double.toString(sum)));

            } else {
                for (Text val : values) {
                    context.write(key, val);
                }
            }

        }
    }


    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Path inputPath;
        Path outputPath;
        int idxInputPath = 1;
        int idxOutputPath = 2;
        Path ptLoopDir;
        FileSystem  fs;
        Job job;
        String[] axis;
        String fixedAxis = "empty";
        int[] sortAxis;
        int count;
        String loopDir;
        FileUtil fu = new FileUtil();;
        FileStatus[] status;

        try {

            if (args[0].indexOf("0") > -1) {
                System.out.println("The first axis is 1.");
                System.exit(1);
            }

            axis = args[0].split(",");

            if (args.length == ARGS_LENGTH) {
                fixedAxis = args[1];
                idxInputPath = 2;
                idxOutputPath = 3;
            } else {
                if (args.length == ARGS_LENGTH - 1) {
                    idxInputPath = 1;
                    idxOutputPath = 2;
                } else {
                    System.out.println("The program supports only 3 or 4 "
                            + "arguments. The given input has " + args.length
                            + " arguments.");
                    System.exit(1);
                }                
            }

            inputPath = new Path(args[idxInputPath]);
            outputPath = new Path(args[idxOutputPath]);
            loopDir = args[idxInputPath] + "LoopDir";

            // Put the input axes in descending order
            sortAxis = new int[axis.length];
            for (int i = 0; i < axis.length; i++) {
                sortAxis[i] = Integer.parseInt(axis[i]);
            }
            Arrays.sort(sortAxis);
            count = 0;
            for (int i = (sortAxis.length - 1); i >= 0; i--) {
                axis[count] = Integer.toString(sortAxis[i]);
                count++;
            }

            fs = FileSystem.get(new URI(outputPath.toString()), conf);

            /*
             * Disable the map output compression in Hadoop for gain of
             * performance
             */
            conf.set("mapred.compress.map.output", "false");

            // Set the key/value separator
            conf.set("mapred.textoutputformat.separator", ",");

            // Set the axes with fixed value
            conf.set("fixedAxis", fixedAxis);

            ptLoopDir = new Path(loopDir);
            fs.delete(ptLoopDir, true);
            fs.mkdirs(ptLoopDir);

            status = fs.listStatus(inputPath);
            for (FileStatus stat : status) {

                fu.copy(fs, stat.getPath(), fs, ptLoopDir, false, true, conf);
            }

            for (int i = 0; i < axis.length; i++) {

                if (i > 0) {

                    fs.delete(ptLoopDir, true);
                    fs.mkdirs(ptLoopDir);
                    status = fs.listStatus(outputPath);
                    for (FileStatus stat : status) {
                        if (stat.getPath().toString().indexOf("part-r-") > -1) {

                            fs.rename(stat.getPath(), ptLoopDir);
                        }
                    }
                    // Use fixedAxis only once.
                    conf.set("fixedAxis", "empty");
                }

                // Delete the output directory if it already exists.
                fs.delete(outputPath, true);
                
                // The chosen axis
                conf.set("axis", axis[i]);

                // Create job
                job = new Job(conf, "SumAxis" + axis[i]);
                job.setJarByClass(SumAxis.class);

                // Specify key / value
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(Text.class);

                // Setup MapReduce job
                job.setMapperClass(Map.class);
                job.setCombinerClass(Combine.class);
                job.setReducerClass(Reduce.class);

                // In the last execution set the number of reduces tasks to one
                if (i == axis.length - 1) {
                    job.setNumReduceTasks(1);
                }

                // Set Map output Key/Value type
                job.setMapOutputKeyClass(Text.class);
                job.setMapOutputValueClass(Text.class);

                job.setInputFormatClass(TextInputFormat.class);
                job.setOutputFormatClass(TextOutputFormat.class);

                // Input
                FileInputFormat.addInputPath(job, ptLoopDir);

                // Output
                FileOutputFormat.setOutputPath(job, outputPath);

                // Execute job
                job.waitForCompletion(true);

            }

            fs.delete(ptLoopDir, true);
            fs.close();

        } catch (Exception e) {
            System.out.println(e);
        }

    }
}

