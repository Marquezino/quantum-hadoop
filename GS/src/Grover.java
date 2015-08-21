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
 * This software calculate one step of the Grover's algorithm using Apache
 * Hadoop.
 *
 * @version 1.1 4 May 2015
 * @author David Souza
 */


public class Grover {

    /**
     * COUNT_PSI: number of parallel calculations. Default value: number
     * of reduce functions.
     */
    private static final long COUNT_PSI = 2;

    public static class Map extends MapReduceBase implements
            Mapper<LongWritable, Text, LongWritable, Text> {

        public void map(LongWritable key, Text value,
                OutputCollector<LongWritable, Text> output, Reporter reporter)
                throws IOException {

            LongWritable outputKey = new LongWritable();
            String[] vals;

            vals = value.toString().split(",");

            if (!(vals[0].equals("#A") || vals[0].equals("#B"))) {

                for (long i = 0; i < COUNT_PSI; i++) {
                    outputKey.set(i);
                    output.collect(outputKey, value);

                }
            } else {
                outputKey.set(-1);
                output.collect(outputKey, value);
            }
        }
    }

    public static class Reduce extends MapReduceBase implements
            Reducer<LongWritable, Text, Text, Text> {
        
        private static Long n;

        public void configure(JobConf job) {
            n = Long.parseLong(job.get("N"));
        }

        public void reduce(LongWritable key, Iterator<Text> values,
                OutputCollector<Text, Text> output, Reporter reporter)
                throws IOException {

            long index = Long.parseLong(key.toString());
            String[] val = new String[1];
            String[] element;
            String[] tempVal;
            Text outputValue = new Text();
            HashMap<Long, String> psi = new HashMap<Long, String>();
            ArrayList<String> listValues = new ArrayList<String>();
            long first = (long)(n / COUNT_PSI * index);
            long last = (long)(n / COUNT_PSI * (index + 1));
            double c1 = 2.0 / n;
            double tempValue = 0;
            long j;
            long m = n / 2;

            if (index != -1) {

                while (values.hasNext()) {
                    listValues.add(values.next().toString());
                }

                for (long i = first; i < last; i++) {

                    for (int idx = 0; idx < listValues.size(); idx++) {

                        val = listValues.get(idx).split(",");
                        j = Long.parseLong(val[1]);

                        if (j == m) {

                            if (i == j) {

                                tempValue = 1.0 - c1;
                            } else {

                                tempValue = -c1;
                            }
                        } else {

                            if (i == j) {

                                tempValue = c1 - 1.0;
                            } else {

                                tempValue = c1;
                            }
                        }

                        element = val[3].split("j");

                        if (psi.containsKey(i)) {
                            tempVal = psi.get(i).split("j");
                            psi.put(i, Double.toString(Double.parseDouble(
                                    tempVal[0]) + tempValue
                                    * Double.parseDouble(element[0]))
                                    + "j" + element[1]);
                        } else {
                            psi.put(i, Double.toString(tempValue
                                    * Double.parseDouble(element[0])) + "j"
                                    + element[1]);
                        }
                    }

                }

                for (long i : psi.keySet()) {

                    outputValue.set(val[0] + "," + Long.toString(i) + ",0,"
                            + psi.get(i));
                    output.collect(null, outputValue);
                }

            } else {
                while (values.hasNext()) {
                    outputValue.set(values.next().toString());
                    output.collect(null, outputValue);
                }
            }
        }
    }


    public static void main(String[] args) throws Exception {

        try {

            // Create the Job and set it name
            JobConf conf = new JobConf(Grover.class);
            conf.setJobName("Grover");

            // Set if the output will be matrix type A ou type B
            //conf.set("type_matrix_output",args[2]);

            Path inputPath = new Path(args[0]);
            Path outputPath = new Path(args[1]);

            // args[2] is the value of n. Set the value of N.
            conf.set("N", Long.toString((long) (Math.pow(2,
                    Integer.parseInt(args[2])))));

            /*
             * Disable the map output compression in Hadoop for gain of
             * performance
             */
            conf.set("mapred.compress.map.output", "false");

            FileSystem  fs = FileSystem.get(new URI(outputPath.toString()),
                                conf);

            // Delete the output directory if it already exists.
            fs.delete(outputPath, true);
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


        } catch (Exception e) {
            System.out.println(e);
        }

    }
}

