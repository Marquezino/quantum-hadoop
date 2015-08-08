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
import java.net.URI;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileUtil;
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
 * This software calculate the Kronecker product between two matrices
 * using Apache Hadoop.
 *
 * @version 1.3 10 Jun 2015
 * @author David Souza
 */


public class KronMatrix {

    /**
     * Kronecker Index Equation:
     *
     * Kronecker row:
     *
     *      If the index start with 1:
     *      matrixA (tensor) matrixB = number_rows(matrixB) * (row_matrixA - 1)
     *                                   + row_matrixB
     *
     *      If the index start with 0:
     *      matrixA (tensor) matrixB = number_rows(matrixB) * (row_matrixA)
     *                                   + row_matrixB
     *
     * Kronecker column:
     *
     *      If the index start with 1:
     *      matrixA (tensor) matrixB = number_columns(matrixB) * (column_matrixA
     *                                  - 1) + column_matrixB
     *
     *      If the index start with 0:
     *      matrixA (tensor) matrixB = number_columns(matrixB)
     *                                  * (column_matrixA) + column_matrixB
     */

    /**
     * Number of elements that will be loaded in the RAM memory in reduce
     * function.
     */
    private static final long NUMBER_ELEMENTS_IN_MEMORY = 1000000;


    public static class Map extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString();
            // "," is the delimiter used in the input file.
            String[] records = line.split(",");
            Text outputKey = new Text();
            Text outputValue = new Text();
            Configuration conf = context.getConfiguration();
            // Number of elements of the matrix A
            long numberElementsA = Long.parseLong(conf.get(
                                        "numberElementsA"));
            // Number of elements of the matrix B
            long numberElementsB = Long.parseLong(conf.get(
                                        "numberElementsB"));
            // Number of parts that the matrix A was splitted
            long numberOfParts = Long.parseLong(conf.get("numberOfParts"));
            long partitionA;


            if (numberElementsB <= NUMBER_ELEMENTS_IN_MEMORY) {

                if (records[0].equals("A")) {    // A is the left matrix.

                    outputKey.set(records[1]);
                    outputValue.set("A," + records[2] + "," + records[3] + ","
                            + records[4]);
                    context.write(outputKey, outputValue);

                } else {

                    if (records[0].equals("B")) {    // B is the right matrix.

                        outputKey.set(records[1]);
                        outputValue.set("B," + records[2] + "," + records[3]
                                + "," + records[4]);
                        context.write(outputKey, outputValue);

                    } else {
                        // #A and #B are the header with the matrix dimension.
                        if ((records[0].equals("#A"))
                                || (records[0].equals("#B"))) {

                            outputKey.set("-1");
                            outputValue.set(records[1] + "," + records[2]);
                            context.write(outputKey, outputValue);
                        }
                    }
                }

            } else {

                if (records[0].equals("A")) {    // A is the left matrix.

                    outputKey.set(records[1]);
                    outputValue.set("A," + records[2] + "," + records[3] + ","
                            + records[4]);
                    context.write(outputKey, outputValue);

                } else {

                    if (records[0].equals("B")) {    // B is the right matrix.

                        partitionA = Long.parseLong(records[1])
                                        % numberOfParts;

                        outputKey.set(Long.toString(partitionA) + "_"
                                + records[1]);
                        outputValue.set("B," + records[2] + "," + records[3]
                                + "," + records[4]);
                        context.write(outputKey, outputValue);

                    } else {
                        // #A and #B are the header with the matrix dimension.
                        if ((records[0].equals("#A"))
                                || (records[0].equals("#B"))) {

                            outputKey.set("-1");
                            outputValue.set(records[1] + "," + records[2]);
                            context.write(outputKey, outputValue);
                        }
                    }
                }


            }

        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            String[] value;
            String[] valA = new String[2];
            String[] valB = new String[2];
            String[] coordinatesA = new String[3];
            ArrayList<Text> valuesCache = new ArrayList<Text>();
            long numberElementsB = 0;
            boolean findA = false;

            Configuration conf = context.getConfiguration();
            String typeMatrixOutput = conf.get("typeMatrixOutput");
            long numberRowsB = Long.parseLong(conf.get("numberRowsB"));
            long numberColumnsB = Long.parseLong(conf.get("numberColumnsB"));
            long totalNumberElementsB = Long.parseLong(conf.get(
                                            "numberElementsB"));
            Text output = new Text();
            String[] fullKey;
            String keyA = "";
            String partA = "";
            long numberOfParts = (totalNumberElementsB
                                    / NUMBER_ELEMENTS_IN_MEMORY);
            long rows = 1;
            long columns = 1;


            if (!(key.toString().equals("-1"))) {

                if (totalNumberElementsB > NUMBER_ELEMENTS_IN_MEMORY) {
                    fullKey = key.toString().split("_");
                    partA = fullKey[0];
                    keyA = fullKey[1];
                }

                for (Text val : values) {

                    numberElementsB++;

                    if (!findA) {
                        value = val.toString().split(",", 2);
                        if (value[0].equals("A")) {
                            value = val.toString().split(",");
                            valA = value[3].split("j");
                            coordinatesA[1] = value[1];
                            coordinatesA[2] = value[2];
                            findA = true;
                            numberElementsB--;
                        }
                    }

                    Text writable = new Text();
                    writable.set(val.toString());
                    valuesCache.add(writable);
                }

                if ((valA[0] == null) && (valA[1] == null)) {
                    throw new IOException("This key haven't a matrix A value");
                }

                for (Text val : valuesCache) {
                    value = val.toString().split(",");
                    if (value[0].equals("B")) {
                        valB = value[3].split("j");

                        output.set(typeMatrixOutput + "," + Long.toString(
                            // Kronecker row equation
                            numberRowsB * Long.parseLong(coordinatesA[1])
                            + Long.parseLong(value[1]))
                            // Kronecker column equation
                            + "," + Long.toString(numberColumnsB
                            * Long.parseLong(coordinatesA[2])
                            + Long.parseLong(value[2]))
                            // Real value
                            + "," + Double.toString(Double.parseDouble(valA[0])
                            * Double.parseDouble(valB[0]))
                            // Imaginary value
                            + "j" + Double.toString(Double.parseDouble(valA[1])
                            * Double.parseDouble(valB[1])));

                        context.write(null, output);

                    }

                }

            } else {

                for (Text val : values) {
                    value = val.toString().split(",");
                    rows *= Long.parseLong(value[0]);
                    columns *= Long.parseLong(value[1]);
                }

                context.write(null, new Text("#" + typeMatrixOutput + ","
                        + Long.toString(rows) + "," + Long.toString(columns)));
            }

        }
    }


    public static long prepareA(Configuration conf, FileSystem fsInput,
            Path inputPath, Path newInputPath) {

        long countA = 0;
        BufferedReader br;
        String oldPath;
        String newPath;
        String line;
        String firstLine;
        boolean newSize;
        boolean verified;
        String[] val;
        String[] tempVal;
        BufferedWriter bw;
        FileStatus[] status;
        FileUtil fu = new FileUtil();

        try {

            // The names of all files in the input path
            status = fsInput.listStatus(inputPath);

            for (int i = 0; i < status.length; i++) {
                br = new BufferedReader(new InputStreamReader(fsInput.open(
                        status[i].getPath())));
                oldPath = status[i].getPath().toString();
                newPath = oldPath.replaceAll(inputPath.toString(),
                            newInputPath.toString());
                firstLine = "splitted";
                newSize = false;
                verified = false;
                line = br.readLine();

                // Empty file. Go to the next.
                if (line == null) {
                    continue;
                }

                if (line.indexOf("A") > -1) {

                    bw = new BufferedWriter(new OutputStreamWriter(
                        fsInput.create(new Path(newPath), true)));

                    while (line != null) {
                        if (!(line.equals("")) && !(String.valueOf(
                                line.charAt(0)).equals(" "))) {

                            val = line.split(",", 3);
                            if (!val[0].equals("#A")) {

                                if (!newSize && !verified) {
                                    tempVal = line.split(",");
                                    verified = true;
                                    if (tempVal.length < 5) {
                                        newSize = true;
                                    }
                                }

                                if (newSize) {
                                    val[0] = "A," + Long.toString(countA) + ",";
                                    if (firstLine.equals("")) {
                                        bw.write("\n" + val[0] + val[1]
                                                + "," + val[2]);
                                    } else {
                                        bw.write(val[0] + val[1]
                                                + "," + val[2]);
                                        firstLine = "";
                                    }

                                } else {
                                    System.out.println("Wrong input format.");
                                    System.exit(1);
                                }

                            } else {
                                bw.write(line);
                                firstLine = "";
                                countA--;
                            }

                            if (newSize || val[1].indexOf("_") == -1) {

                                countA++;

                            } else {
                                tempVal = val[1].split("_");
                                if (countA < Long.parseLong(tempVal[1])) {
                                    countA = Long.parseLong(tempVal[1]) + 1;
                                }
                            }
                        }

                        line = br.readLine();

                    }

                    if (newSize) {
                        bw.close();

                    } else {
                        bw.close();
                        fsInput.delete(new Path(newPath), true);
                        fu.copy(fsInput, status[i].getPath(), fsInput,
                                newInputPath, false, true, conf);
                    }

                }

                br.close();

            }


        } catch (Exception e) {
            System.out.println(e);
        }

        return countA;
    }


    public static long prepareB(Configuration conf, FileSystem fsInput,
            Path inputPath, Path newInputPath, long countA) {

        long countB = 0;
        BufferedReader br;
        String oldPath;
        String newPath;
        String line;
        String firstLine;
        boolean newSize;
        boolean verified;
        String[] val;
        String[] tempVal;
        BufferedWriter bw;
        FileStatus[] status;
        FileUtil fu = new FileUtil();

        try {

            // The names of all files in the input path
            status = fsInput.listStatus(inputPath);
            for (int i = 0; i < status.length; i++) {
                br = new BufferedReader(new InputStreamReader(fsInput.open(
                        status[i].getPath())));
                oldPath = status[i].getPath().toString();
                newPath = oldPath.replaceAll(inputPath.toString(),
                            newInputPath.toString());
                firstLine = "splitted";
                newSize = false;
                verified = false;
                line = br.readLine();

                // Empty file. Go to the next.
                if (line == null) {
                    continue;
                }

                if (line.indexOf("#B") > -1) {
                    String[] vals = line.split(",");
                    conf.set("numberRowsB", vals[1]);
                    conf.set("numberColumnsB", vals[2]);
                }

                if (line.indexOf("B") > -1) {

                    bw = new BufferedWriter(new OutputStreamWriter(
                        fsInput.create(new Path(newPath), true)));

                    while (line != null) {
                        if (!(line.equals("")) && !(String.valueOf(
                                line.charAt(0)).equals(" "))) {

                            val = line.split(",", 2);

                            if (!val[0].equals("#B")) {

                                if (!newSize && !verified) {
                                    tempVal = line.split(",");
                                    verified = true;
                                    if (tempVal.length < 5) {
                                        newSize = true;
                                    }
                                }

                                if (newSize) {
                                    if (firstLine.equals("")) {
                                        for (long j = 0; j < countA; j++) {
                                            val[0] = "B," + Long.toString(j)
                                                        + ",";
                                            bw.write("\n" + val[0] + val[1]);
                                        }

                                    } else {
                                        val[0] = "B,0,";
                                        bw.write(val[0] +   val[1]);
                                        for (long j = 1; j < countA; j++) {
                                            val[0] = "B," + Long.toString(j)
                                                        + ",";
                                            bw.write("\n" + val[0] + val[1]);
                                        }
                                        firstLine = "";
                                    }
                                } else {
                                    System.out.println("Wrong input format.");
                                    System.exit(1);
                                }

                            } else {
                                bw.write(line);
                                firstLine = "";
                                countB--;
                            }

                            countB++;
                        }

                        line = br.readLine();

                    }

                    if (newSize) {
                        bw.close();

                    } else {
                        bw.close();
                        fsInput.delete(new Path(newPath), true);
                        fu.copy(fsInput, status[i].getPath(), fsInput,
                                newInputPath, false, true, conf);
                    }

                }

                br.close();

            }


        } catch (Exception e) {
            System.out.println(e);
        }

        return countB;
    }


    public static long replicateA(Configuration conf, FileSystem fsInput,
            Path inputPath, Path newInputPath, long countB) {

        long numberOfParts = 1;
        BufferedReader br;
        String oldPath;
        String newPath;
        String line;
        String firstLine;
        boolean newSize;
        boolean verified;
        String[] val;
        String[] tempVal;
        BufferedWriter bw;
        FileStatus[] status;
        String temp;
        FileUtil fu = new FileUtil();

        try {

            // The names of all files in the input path
            status = fsInput.listStatus(newInputPath);
            if (countB > NUMBER_ELEMENTS_IN_MEMORY) {

                if (countB % NUMBER_ELEMENTS_IN_MEMORY == 0) {
                    numberOfParts = (countB / NUMBER_ELEMENTS_IN_MEMORY);
                } else {
                    numberOfParts = (countB / NUMBER_ELEMENTS_IN_MEMORY) + 1;
                }

                for (int i = 0; i < status.length; i++) {
                    br = new BufferedReader(new InputStreamReader(fsInput.open(
                            status[i].getPath())));
                    oldPath = status[i].getPath().toString();
                    newPath = oldPath + "InPieces";
                    firstLine = "splitted";
                    newSize = false;
                    verified = false;
                    line = br.readLine();

                    // Empty file. Go to the next.
                    if (line == null) {
                        continue;
                    }

                    if (line.indexOf("A") > -1) {

                        bw = new BufferedWriter(new OutputStreamWriter(
                            fsInput.create(new Path(newPath), true)));

                        while (line != null) {
                            if (!(line.equals("")) && !(String.valueOf(
                                    line.charAt(0)).equals(" "))) {

                                val = line.split(",", 3);
                                if (!val[0].equals("#A")) {

                                    if (!newSize && !verified) {
                                        tempVal = line.split(",");
                                        verified = true;
                                        if (tempVal.length == 5) {
                                            newSize = true;
                                        }
                                    }

                                    if (newSize) {
                                        if (firstLine.equals("")) {
                                            for (long j = 0; j < numberOfParts;
                                                    j++) {
                                                temp = "," + Long.toString(j)
                                                        + "_" + val[1] + ",";
                                                bw.write("\n" + val[0] + temp
                                                        + val[2]);
                                            }
                                        } else {
                                            temp = ",0_" + val[1] + ",";
                                            bw.write(val[0] + temp + val[2]);
                                            for (long j = 1; j < numberOfParts;
                                                    j++) {
                                                temp = "," + Long.toString(j)
                                                        + "_" + val[1] + ",";
                                                bw.write("\n" + val[0] + temp
                                                        + val[2]);
                                            }
                                            firstLine = "";
                                        }

                                    } else {
                                        System.out.println("Wrong input format."
                                                );
                                        System.exit(1);
                                    }

                                } else {
                                    bw.write(line);
                                    firstLine = "";
                                }

                            }
                            if (newSize) {
                                line = br.readLine();
                            } else {
                                line = null;
                            }

                        }

                        if (newSize) {
                            bw.close();
                            // Delete the old file not splitted.
                            fsInput.delete(status[i].getPath(), true);

                        } else {
                            bw.close();
                            fsInput.delete(new Path(newPath), true);
                            fu.copy(fsInput, status[i].getPath(), fsInput,
                                newInputPath, false, true, conf);
                        }

                    }

                    br.close();

                }

            }


        } catch (Exception e) {
            System.out.println(e);
        }

        return numberOfParts;
    }


    public static void main(String[] args) throws Exception {

        long countA = 0;
        long countB = 0;
        long numberOfParts = 1;
        Configuration conf = new Configuration();
        Path inputPath;
        Path outputPath;
        Path newInputPath;
        FileSystem fsInput;
        FileSystem  fs;


        try {

            inputPath = new Path(args[0]);
            outputPath = new Path(args[1]);
            newInputPath = new Path(inputPath.toString() + "NewFormat");

            // Set if the output will be matrix type A ou type B
            conf.set("typeMatrixOutput", args[2]);

            fsInput = FileSystem.get(conf);

            // Create a new input folder for the new format files
            fsInput.delete(newInputPath, true);
            fsInput.mkdirs(newInputPath);

            countA = prepareA(conf, fsInput, inputPath, newInputPath);

            countB = prepareB(conf, fsInput, inputPath, newInputPath, countA);

            numberOfParts = replicateA(conf, fsInput, inputPath, newInputPath,
                                countB);


            fsInput.close();

            // Set the number of elements of the matrix A
            conf.set("numberElementsA", Long.toString(countA));
            // Set the number of elements of the matrix B
            conf.set("numberElementsB", Long.toString(countB));

            // Set the number of parts that the matrix A was splitted
            conf.set("numberOfParts", Long.toString(numberOfParts));


            fs = FileSystem.get(new URI(outputPath.toString()), conf);

            // Delete the output directory if it already exists.
            fs.delete(outputPath, true);
            //fs.close();

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
            FileInputFormat.addInputPath(job, newInputPath);

            // Output
            FileOutputFormat.setOutputPath(job, outputPath);

            // Execute job
            job.waitForCompletion(true);

            // Delete new input format folder
            fs.delete(newInputPath, true);
            fs.close();

        } catch (Exception e) {
            System.out.println(e);
        }

    }
}

