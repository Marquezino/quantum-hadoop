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
import java.util.ArrayList;
import java.util.List;
import java.net.URI;
import java.io.BufferedReader;
import java.io.InputStreamReader;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


/**
 * This software calculate the multiplication between two matrices using
 * Apache Hadoop.
 *
 * @version 1.0 7 Mar 2015
 * @author David Souza
 */


public class MultMatrix {

    public static class MapPrepare extends
            Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString();
            // "," is the delimiter used in the input file.
            String[] records = line.split(",");
            Text outputKey = new Text();
            Text outputValue = new Text();

            if (records[0].equals("A")) {    // A is the left matrix.

                outputKey.set(records[2]);
                outputValue.set("A," + records[1] + "," + records[2]
                            + "," + records[3]);
                context.write(outputKey, outputValue);

            } else {
                if (records[0].equals("B")) {    // B is the right matrix.

                    outputKey.set(records[1]);
                    outputValue.set("B," + records[1] + "," + records[2]
                            + "," + records[3]);
                    context.write(outputKey, outputValue);
                } else {
                    // # is the line with the matrices dimensions.
                    if (records[0].indexOf("#") > -1) {

                        outputKey.set("#");
                        outputValue.set(records[0] + "," + records[1]
                                + "," + records[2]);
                        context.write(outputKey, outputValue);
                    }
                }
            }
        }
    }


    public static class ReducePrepare extends
            Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            String[] value;
            String[] valA;
            String[] valB;
            String rows = "";
            String columns = "";
            List<String[]> listA = new ArrayList<String[]>();
            List<String[]> listB = new ArrayList<String[]>();
            Text output = new Text();

            for (Text val : values) {
                value = val.toString().split(",");
                if (value[0].equals("A")) {
                    listA.add(new String[]{value[1], value[2], value[3]});

                } else {
                    if (value[0].equals("B")) {
                        listB.add(new String[]{value[1], value[2], value[3]});

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

            if ((!rows.equals("")) && (!columns.equals(""))) {
                context.write(null, new Text(key.toString() + ";" + rows
                        + "," + columns));
            }

            for (String[] elementA : listA) {
                for (String[] elementB : listB) {

                    valA = elementA[2].split("j");
                    valB = elementB[2].split("j");

                    output.set(elementA[0] + "," + elementB[1] + ";"
                            + Double.toString(Double.parseDouble(valA[0])
                            * Double.parseDouble(valB[0])) + "j"
                            + Double.toString(Double.parseDouble(valA[1])
                            * Double.parseDouble(valB[1])));

                    context.write(null, output);
                }
            }

        }
    }


    public static class MapResult extends
            Mapper<LongWritable, Text, Text, Text> {

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String[] vals = value.toString().split(";");

            context.write(new Text(vals[0]), new Text(vals[1]));

        }
    }


    public static class CombineResult extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            String[] value;
            double real = 0.0d;
            double imaginary = 0.0d;

            if (!key.toString().equals("#")) {

                for (Text val : values) {
                    value = val.toString().split("j");
                    real += Double.parseDouble(value[0]);
                    imaginary += Double.parseDouble(value[1]);
                }

                if ((real != 0.0d) || (imaginary != 0.0d)) {

                    context.write(key, new Text(Double.toString(real) + "j"
                            + Double.toString(imaginary)));

                }

            } else {
                for (Text val : values) {
                    context.write(key, val);
                }
            }


        }
    }


    public static class ReduceResult extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            String[] value;
            double real = 0.0d;
            double imaginary = 0.0d;
            Configuration conf = context.getConfiguration();
            String typeMatrixOutput = conf.get("typeMatrixOutput");

            if (!key.toString().equals("#")) {

                for (Text val : values) {
                    value = val.toString().split("j");
                    real += Double.parseDouble(value[0]);
                    imaginary += Double.parseDouble(value[1]);
                }

                if (real != 0.0d || imaginary != 0.0d) {

                    context.write(null, new Text(typeMatrixOutput + ","
                            + key.toString() + "," + Double.toString(real)
                            + "j" + Double.toString(imaginary)));
                }

            } else {
                for (Text val : values) {
                    value = val.toString().split(",");
                    // Write the matrix dimension in the file.
                    context.write(null, new Text("#" + typeMatrixOutput + ","
                            + value[0] + "," + value[1]));
                }
            }

        }
    }


    public static void main(String[] args) throws Exception {

        String[] dimA = new String[2];
        String[] dimB = new String[2];
        BufferedReader br;
        String tempPath = args[0] + "TempSecondPass";
        String line;
        String[] vals;

        Configuration conf = new Configuration();
        Path inputPath;
        Path outputPath;
        FileSystem fsInput;
        FileStatus[] status;
        FileSystem fs;
        Job job;
        Path inputPath2;
        Path outputPath2;
        FileSystem fs2;
        Job job2;


        try {

            // Set if the output will be matrix type A ou type B
            conf.set("typeMatrixOutput", args[2]);

            // The input/output paths of the first Map/Reduce job
            inputPath = new Path(args[0]);
            outputPath = new Path(tempPath);

            // The input/output paths of the second Map/Reduce job
            inputPath2 = new Path(tempPath);
            outputPath2 = new Path(args[1]);

            fsInput = FileSystem.get(conf);
            // The names of all files in the input path
            status = fsInput.listStatus(inputPath);

            for (int i = 0; i < status.length; i++) {
                br = new BufferedReader(new InputStreamReader(fsInput.open(
                        status[i].getPath())));
                line = br.readLine();

                // Empty file. Go to the next.
                if (line == null) {
                    br.close();
                    continue;
                }

                if (dimA[0] != null && dimA[1] != null && dimB[0] != null
                        && dimB[1] != null) {
                    br.close();
                    break;
                }

                if (line.indexOf("#") > -1) {
                    if (line.indexOf("#A") > -1) {
                        vals = line.split(",");
                        dimA[0] = vals[1];
                        dimA[1] = vals[2];

                    } else {
                        if (line.indexOf("#B") > -1) {
                            vals = line.split(",");
                            dimB[0] = vals[1];
                            dimB[1] = vals[2];
                        }

                    }
                }
                br.close();
            }

            fsInput.close();

            /*
             * A_(m x n) and B_(p x q) -> verify if n==p,
             * constraint for matrix multiplication
             */
            if (!dimA[1].equals(dimB[0])) {

                System.out.print("\n---------------------------------------"
                        + "-------------------------\n");
                System.out.println("You can not perform the multiplication "
                        + "between the matrices in the input path. The "
                        + "constraint below should be satisfied:\nThe "
                        + "number of columns in the matrix A must be "
                        + "equal to the number of rows in the matrix B.");
                System.out.println("---------------------------------------"
                        + "-------------------------\n");
                System.exit(1);

            }


            fs = FileSystem.get(new URI(outputPath.toString()), conf);

            // Delete the output directory if it already exists.
            fs.delete(outputPath, true);
            fs.close();

            // Create job
            job = new Job(conf, "MatrixMultiplicationStep1");
            job.setJarByClass(MultMatrix.class);

            // Specify key / value
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            // Setup MapReduce job
            job.setMapperClass(MapPrepare.class);
            job.setReducerClass(ReducePrepare.class);

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

            // Start of the second Map/Reduce job
            fs2 = FileSystem.get(new URI(outputPath2.toString()), conf);

            // Delete the output directory if it already exists.
            fs2.delete(outputPath2, true);

            // Create job
            job2 = new Job(conf, "MatrixMultiplicationStep2");
            job2.setJarByClass(MultMatrix.class);

            // Specify key / value
            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(Text.class);

            // Setup MapReduce job
            job2.setMapperClass(MapResult.class);
            job2.setCombinerClass(CombineResult.class);
            job2.setReducerClass(ReduceResult.class);

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
            fs2.delete(inputPath2, true);
            fs2.close();

        } catch (Exception e) {
            System.out.println(e);
        }

    }
}
