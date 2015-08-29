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


package qwd;

import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.io.File;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.conf.Configuration;


/**
 * This software generates data to be used in Quandoop to simulate a
 * one-dimensional quantum walk with four particles.
 *
 * @version 1.0 25 Aug 2015
 * @author David Souza
 */


public class QWD {

    /**
     * The dimensions of the two dimensional lattice.
     */
    private static final int SIZE = 5;

    /**
     * The path in the HDFS where the program will store the data.
     */
    private static final String WORK_DIR = "qwd_tmp/";

    /**
     * The folder path where the .jar files are stored.
     */
    private static final String JAR_DIR = "/home/david/Desktop/java/QWD/";

    /**
     * The folder path where the result will be stored. Should be a empty folder
     * because this ALL DATA will be deleted.
     */
    private static final String OUTPUT_DIR =
            "/home/david/Desktop/java/QWD/input/";

    /*
     * Set for true to delete unnecessary files during the execution to increase
     * available storage space.
     */
    private static final boolean CLEAN_FOLDERS = true;


    public static void main(String[] args) throws Exception {

        long startTime;
        long psiCount;
        String hadamard;
        String identity;
        String operatorCoinW0;
        String operatorShiftW0;
        int cfJ;
        String operatorW0A;
        String operatorW0B;
        String identityA1;
        String identityB1;
        String identityA2;
        String identityB2;
        String identityA3;
        String identityB3;
        String operatorW1;
        String operatorW2;
        String operatorW3;
        String operatorW4;
        String psi;
        BufferedWriter bw;
        Runtime rt = Runtime.getRuntime();
        Process pr;
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);;
        FileUtil fu = new FileUtil();;
        Path pt;
        FileStatus[] status;

        try {

            startTime = System.nanoTime();

            System.out.println("The matrices are being generated...");

            /*
             * Delete the WORK_DIR directory if exists. And create a new one
             * empty.
             */
            pt = new Path(WORK_DIR);
            fs.delete(pt, true);
            fs.mkdirs(pt);

            // Start of the operatorCoinW0
            hadamard = "hadamard";
            pt = new Path(WORK_DIR + hadamard);
            bw = new BufferedWriter(new OutputStreamWriter(fs.create(pt,
                    true)));
            bw.write("#A,2,2\n");
            bw.write("A,0,0," + Double.toString(1.0 / Math.sqrt(2)) + "j0\n");
            bw.write("A,0,1," + Double.toString(1.0 / Math.sqrt(2)) + "j0\n");
            bw.write("A,1,0," + Double.toString(1.0 / Math.sqrt(2)) + "j0\n");
            bw.write("A,1,1," + Double.toString(-1.0 / Math.sqrt(2)) + "j0");
            bw.close();

            identity = "identity";
            pt = new Path(WORK_DIR + identity);
            bw = new BufferedWriter(new OutputStreamWriter(fs.create(pt,
                    true)));
            bw.write("#B," + Integer.toString(SIZE) + ","
                    + Integer.toString(SIZE));

            for (int i = 0; i < SIZE; i++) {
                bw.write("\nB," + Integer.toString(i) + ","
                        + Integer.toString(i) + ",1.0j0");
            }
            bw.close();


            operatorCoinW0 = "operatorCoinW0";

            // Delete the output directory if exists.
            pt = new Path(WORK_DIR + operatorCoinW0);
            fs.delete(pt, true);

            // Delete the input directory if exists and create a new one.
            pt = new Path(WORK_DIR + operatorCoinW0 + "_input");
            fs.delete(pt, true);
            fs.mkdirs(pt);

            // Move hadamard and identity to operatorCoinW0_input
            fs.rename(new Path(WORK_DIR + hadamard), pt);
            fs.rename(new Path(WORK_DIR + identity), pt);

            pr = rt.exec("hadoop jar " + JAR_DIR + "operations.jar operations."
                    + "KronMatrix " + WORK_DIR + operatorCoinW0 + "_input" + " "
                    + WORK_DIR + operatorCoinW0 + " B");

            pr.waitFor();
            pr.destroy();

            if (CLEAN_FOLDERS) {
                fs.delete(pt, true);
            }

            System.out.println("End of the operatorCoinW0.");

            /*
             * End of the operatorCoinW0. Start of the operatorShiftW0,
             * operatorW0A and operatorW0B.
             */ 
            operatorShiftW0 = "operatorShiftW0";
            pt = new Path(WORK_DIR + operatorShiftW0);
            bw = new BufferedWriter(new OutputStreamWriter(fs.create(pt,
                    true)));
            bw.write("#A," + Long.toString((long) (2 * SIZE))
                    + "," + Long.toString((long) (2 * SIZE)));

            cfJ = 0;
            for (int j = 0; j < 2; j++) {
                for (int x = 0; x < SIZE; x++) {

                    if (x + Math.pow(-1, j) < 0) {
                        cfJ = SIZE;
                    } else {
                        cfJ = 0;
                    }

                    bw.write("\nA," + Long.toString((long) (j * SIZE + x)) + ","
                            + Long.toString((long) ((j * SIZE) + (cfJ + (x
                            + Math.pow(-1, j)) % SIZE))) + ",1.0j0");
                }
            }
            bw.close();

            operatorW0A = "operatorW0A";

            // Delete the output directory if exists.
            pt = new Path(WORK_DIR + operatorW0A);
            fs.delete(pt, true);

            // Delete the input directory if exists and create a new one.
            pt = new Path(WORK_DIR + operatorW0A + "_input");
            fs.delete(pt, true);
            fs.mkdirs(pt);


            fs.rename(new Path(WORK_DIR + operatorShiftW0), pt);

            status = fs.listStatus(new Path(WORK_DIR + operatorCoinW0));
            for (FileStatus stat : status) {

                if (stat.getPath().toString().indexOf("part-") > -1) {

                    fs.rename(stat.getPath(), pt);
                }

            }

            pr = rt.exec("hadoop jar " + JAR_DIR + "operations.jar operations."
                    + "MultMatrix " + WORK_DIR + operatorW0A + "_input" + " "
                    + WORK_DIR + operatorW0A + " A");

            pr.waitFor();
            pr.destroy();

            operatorW0B = "operatorW0B";

            // Delete the output directory if exists.
            pt = new Path(WORK_DIR + operatorW0B);
            fs.delete(pt, true);

            pr = rt.exec("hadoop jar " + JAR_DIR + "operations.jar operations."
                    + "MultMatrix " + WORK_DIR + operatorW0A + "_input" + " "
                    + WORK_DIR + operatorW0B + " B");

            pr.waitFor();
            pr.destroy();

            if (CLEAN_FOLDERS) {
                pt = new Path(WORK_DIR + operatorCoinW0);
                fs.delete(pt, true);
                pt = new Path(WORK_DIR + operatorW0A + "_input");
                fs.delete(pt, true);
            }

            System.out.println("End of the operatorW0A and operatorW0B.");

            /*
             * End of the operatorW0A and operatorW0B. Start of the
             * operatorW1, operatorW2, operatorW3 and operatorW4.
             */

            identityA1 = "identityA1";

            pt = new Path(WORK_DIR + identityA1);
            bw = new BufferedWriter(new OutputStreamWriter(fs.create(pt,
                    true)));
            bw.write("#A," + Integer.toString(2 * SIZE) + ","
                    + Integer.toString(2 * SIZE));

            for (int i = 0; i < 2 * SIZE; i++) {
                bw.write("\nA," + Integer.toString(i) + ","
                        + Integer.toString(i) + ",1.0j0");
            }
            bw.close();

            identityB1 = "identityB1";

            pt = new Path(WORK_DIR + identityB1);
            bw = new BufferedWriter(new OutputStreamWriter(fs.create(pt,
                    true)));
            bw.write("#B," + Integer.toString(2 * SIZE) + ","
                    + Integer.toString(2 * SIZE));

            for (int i = 0; i < 2 * SIZE; i++) {
                bw.write("\nB," + Integer.toString(i) + ","
                        + Integer.toString(i) + ",1.0j0");
            }
            bw.close();

            identityA2 = "identityA2";

            pt = new Path(WORK_DIR + identityA2);
            bw = new BufferedWriter(new OutputStreamWriter(fs.create(pt,
                    true)));
            bw.write("#A," + Integer.toString(4 * SIZE * SIZE) + ","
                    + Integer.toString(4 * SIZE * SIZE));

            for (int i = 0; i < 4 * SIZE * SIZE; i++) {
                bw.write("\nA," + Integer.toString(i) + ","
                        + Integer.toString(i) + ",1.0j0");
            }
            bw.close();

            identityB2 = "identityB2";

            pt = new Path(WORK_DIR + identityB2);
            bw = new BufferedWriter(new OutputStreamWriter(fs.create(pt,
                    true)));
            bw.write("#B," + Integer.toString(4 * SIZE * SIZE) + ","
                    + Integer.toString(4 * SIZE * SIZE));

            for (int i = 0; i < 4 * SIZE * SIZE; i++) {
                bw.write("\nB," + Integer.toString(i) + ","
                        + Integer.toString(i) + ",1.0j0");
            }
            bw.close();

            identityA3 = "identityA3";

            pt = new Path(WORK_DIR + identityA3);
            bw = new BufferedWriter(new OutputStreamWriter(fs.create(pt,
                    true)));
            bw.write("#A," + Integer.toString(8 * SIZE * SIZE * SIZE) + ","
                    + Integer.toString(8 * SIZE * SIZE * SIZE));

            for (int i = 0; i < 8 * SIZE * SIZE * SIZE; i++) {
                bw.write("\nA," + Integer.toString(i) + ","
                        + Integer.toString(i) + ",1.0j0");
            }
            bw.close();

            identityB3 = "identityB3";

            pt = new Path(WORK_DIR + identityB3);
            bw = new BufferedWriter(new OutputStreamWriter(fs.create(pt,
                    true)));
            bw.write("#B," + Integer.toString(8 * SIZE * SIZE * SIZE) + ","
                    + Integer.toString(8 * SIZE * SIZE * SIZE));

            for (int i = 0; i < 8 * SIZE * SIZE * SIZE; i++) {
                bw.write("\nB," + Integer.toString(i) + ","
                        + Integer.toString(i) + ",1.0j0");
            }
            bw.close();

            operatorW1 = "operatorW1";

            // Delete the output directory if exists.
            pt = new Path(WORK_DIR + operatorW1);
            fs.delete(pt, true);

            // Delete the input directory if exists and create a new one.
            pt = new Path(WORK_DIR + operatorW1 + "_input");
            fs.delete(pt, true);
            fs.mkdirs(pt);

            fs.rename(new Path(WORK_DIR + identityB3), pt);

            status = fs.listStatus(new Path(WORK_DIR + operatorW0A));
            for (FileStatus stat : status) {

                if (stat.getPath().toString().indexOf("part-") > -1) {

                    fs.rename(stat.getPath(), pt);
                }

            }

            pr = rt.exec("hadoop jar " + JAR_DIR + "operations.jar operations."
                    + "KronMatrix " + WORK_DIR + operatorW1 + "_input" + " "
                    + WORK_DIR + operatorW1 + " A");

            pr.waitFor();
            pr.destroy();

            System.out.println("End of the operatorW1.");

            operatorW2 = "operatorW2";

            // Delete the output directory if exists.
            pt = new Path(WORK_DIR + operatorW2);
            fs.delete(pt, true);

            // Delete the input directory if exists and create a new one.
            pt = new Path(WORK_DIR + operatorW2 + "_input");
            fs.delete(pt, true);
            fs.mkdirs(pt);

            fs.rename(new Path(WORK_DIR + identityA3), pt);

            status = fs.listStatus(new Path(WORK_DIR + operatorW0B));
            for (FileStatus stat : status) {

                if (stat.getPath().toString().indexOf("part-") > -1) {

                    fu.copy(fs, stat.getPath(), fs, pt, false, true, conf);
                }

            }

            pr = rt.exec("hadoop jar " + JAR_DIR + "operations.jar operations."
                    + "KronMatrix " + WORK_DIR + operatorW2 + "_input" + " "
                    + WORK_DIR + operatorW2 + " A");

            pr.waitFor();
            pr.destroy();

            System.out.println("End of the operatorW2.");

            operatorW3 = "operatorW3";

            // Delete the output directory if exists.
            pt = new Path(WORK_DIR + operatorW3);
            fs.delete(pt, true);

            // Delete the input directory if exists and create a new one.
            pt = new Path(WORK_DIR + operatorW3 + "_input");
            fs.delete(pt, true);
            fs.mkdirs(pt);

            fs.rename(new Path(WORK_DIR + identityA2), pt);

            status = fs.listStatus(new Path(WORK_DIR + operatorW0B));
            for (FileStatus stat : status) {

                if (stat.getPath().toString().indexOf("part-") > -1) {

                    fu.copy(fs, stat.getPath(), fs, pt, false, true, conf);
                }

            }

            pr = rt.exec("hadoop jar " + JAR_DIR + "operations.jar operations."
                    + "KronMatrix " + WORK_DIR + operatorW3 + "_input" + " "
                    + WORK_DIR + operatorW3+ "_temp" + " A");

            pr.waitFor();
            pr.destroy();

            pt = new Path(WORK_DIR + operatorW3 + "_input");
            fs.delete(pt, true);
            fs.rename(new Path(WORK_DIR + operatorW3+ "_temp"), pt);
            pt = new Path(WORK_DIR + operatorW3 + "_input/_logs");
            fs.delete(pt, true);
            pt = new Path(WORK_DIR + operatorW3 + "_input/_SUCCESS");
            fs.delete(pt, true);
            pt = new Path(WORK_DIR + operatorW3 + "_input");

            fs.rename(new Path(WORK_DIR + identityB1), pt);

            pr = rt.exec("hadoop jar " + JAR_DIR + "operations.jar operations."
                    + "KronMatrix " + WORK_DIR + operatorW3 + "_input" + " "
                    + WORK_DIR + operatorW3 + " A");

            pr.waitFor();
            pr.destroy();
           
            System.out.println("End of the operatorW3.");

            operatorW4 = "operatorW4";

            // Delete the output directory if exists.
            pt = new Path(WORK_DIR + operatorW4);
            fs.delete(pt, true);

            // Delete the input directory if exists and create a new one.
            pt = new Path(WORK_DIR + operatorW4 + "_input");
            fs.delete(pt, true);
            fs.mkdirs(pt);

            fs.rename(new Path(WORK_DIR + identityA1), pt);

            status = fs.listStatus(new Path(WORK_DIR + operatorW0B));
            for (FileStatus stat : status) {

                if (stat.getPath().toString().indexOf("part-") > -1) {

                    fu.copy(fs, stat.getPath(), fs, pt, false, true, conf);
                }

            }

            pr = rt.exec("hadoop jar " + JAR_DIR + "operations.jar operations."
                    + "KronMatrix " + WORK_DIR + operatorW4 + "_input" + " "
                    + WORK_DIR + operatorW4+ "_temp" + " A");

            pr.waitFor();
            pr.destroy();

            pt = new Path(WORK_DIR + operatorW4 + "_input");
            fs.delete(pt, true);
            fs.rename(new Path(WORK_DIR + operatorW4+ "_temp"), pt);
            pt = new Path(WORK_DIR + operatorW4 + "_input/_logs");
            fs.delete(pt, true);
            pt = new Path(WORK_DIR + operatorW4 + "_input/_SUCCESS");
            fs.delete(pt, true);
            pt = new Path(WORK_DIR + operatorW4 + "_input");

            fs.rename(new Path(WORK_DIR + identityB2), pt);

            pr = rt.exec("hadoop jar " + JAR_DIR + "operations.jar operations."
                    + "KronMatrix " + WORK_DIR + operatorW4 + "_input" + " "
                    + WORK_DIR + operatorW4 + " A");

            pr.waitFor();
            pr.destroy();
            
            System.out.println("End of the operatorW4.");

            if (CLEAN_FOLDERS) {
                pt = new Path(WORK_DIR + operatorW0A);
                fs.delete(pt, true);
                pt = new Path(WORK_DIR + operatorW0B);
                fs.delete(pt, true);
                pt = new Path(WORK_DIR + operatorW1 + "_input");
                fs.delete(pt, true);
                pt = new Path(WORK_DIR + operatorW2 + "_input");
                fs.delete(pt, true);
                pt = new Path(WORK_DIR + operatorW3 + "_input");
                fs.delete(pt, true);
                pt = new Path(WORK_DIR + operatorW4 + "_input");
                fs.delete(pt, true);
            }

            /*
             * End of the operatorW1, operatorW2, operatorW3 and operatorW4.
             * Start of the psi.
             */

            psi = "psi";

            pt = new Path(WORK_DIR + psi);
            fs.delete(pt, true);
            fs.mkdirs(pt);
            pt = new Path(WORK_DIR + psi + "/part-r");

            bw = new BufferedWriter(new OutputStreamWriter(fs.create(pt,
                    true)));
            bw.write("#B," + Long.toString((long) (16 * Math.pow(SIZE, 4)))
                    + ",1");

            psiCount = 0;
            for (int k1 = 0; k1 < 2; k1++) {
                for (int k2 = 0; k2 < SIZE; k2++) {
                    for (int k3 = 0; k3 < 2; k3++) {
                        for (int k4 = 0; k4 < SIZE; k4++) {
                            for (int k5 = 0; k5 < 2; k5++) {
                                for (int k6 = 0; k6 < SIZE; k6++) {
                                    for (int k7 = 0; k7 < 2; k7++) {
                                        for (int k8 = 0; k8 < SIZE; k8++) {

                                            if ((k1 == 0) && (k2 == (int)SIZE/2)
                                                    && (k3 == 1)
                                                    && (k4 == (int)SIZE/2)
                                                    && (k5 == 0)
                                                    && (k6 == (int)SIZE/2)
                                                    && (k7 == 0)
                                                    && (k8 == (int)SIZE/2)) {

                                                bw.write("\nB," + Long.toString(
                                                        psiCount) + ",0,"
                                                        + Double.toString(1.0
                                                        / Math.sqrt(4)) + "j0");

                                            }

                                            if ((k1 == 1) && (k2 == (int)SIZE/2)
                                                    && (k3 == 0)
                                                    && (k4 == (int)SIZE/2)
                                                    && (k5 == 0)
                                                    && (k6 == (int)SIZE/2)
                                                    && (k7 == 0)
                                                    && (k8 == (int)SIZE/2)) {

                                                bw.write("\nB," + Long.toString(
                                                        psiCount)
                                                        + ",0,"
                                                        + Double.toString(-1.0
                                                        / Math.sqrt(4)) + "j0");

                                            }

                                            if ((k1 == 0) && (k2 == (int)SIZE/2)
                                                    && (k3 == 0)
                                                    && (k4 == (int)SIZE/2)
                                                    && (k5 == 0)
                                                    && (k6 == (int)SIZE/2)
                                                    && (k7 == 1)
                                                    && (k8 == (int)SIZE/2)) {

                                                bw.write("\nB," + Long.toString(
                                                        psiCount) + ",0,"
                                                        + Double.toString(1.0
                                                        / Math.sqrt(4)) + "j0");

                                            }

                                            if ((k1 == 0) && (k2 == (int)SIZE/2)
                                                    && (k3 == 0)
                                                    && (k4 == (int)SIZE/2)
                                                    && (k5 == 1)
                                                    && (k6 == (int)SIZE/2)
                                                    && (k7 == 0)
                                                    && (k8 == (int)SIZE/2)) {

                                                bw.write("\nB," + Long.toString(
                                                        psiCount) + ",0,"
                                                        + Double.toString(-1.0
                                                        / Math.sqrt(4)) + "j0");

                                            }

                                            psiCount++;

                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }

            bw.close();

            System.out.println("End of the psi.");

            // Delete the OUTPUT_DIR if it exists.
            fu.fullyDelete(new File(OUTPUT_DIR));

            /*
             * Delete _logs folder and _SUCCESS file before copy the data from
             * HDFS to local.
             */
            pt = new Path(WORK_DIR + operatorW1 + "/_logs");
            fs.delete(pt, true);
            pt = new Path(WORK_DIR + operatorW1 + "/_SUCCESS");
            fs.delete(pt, false);
            pt = new Path(WORK_DIR + operatorW2 + "/_logs");
            fs.delete(pt, true);
            pt = new Path(WORK_DIR + operatorW2 + "/_SUCCESS");
            fs.delete(pt, false);
            pt = new Path(WORK_DIR + operatorW3 + "/_logs");
            fs.delete(pt, true);
            pt = new Path(WORK_DIR + operatorW3 + "/_SUCCESS");
            fs.delete(pt, false);
            pt = new Path(WORK_DIR + operatorW4 + "/_logs");
            fs.delete(pt, true);
            pt = new Path(WORK_DIR + operatorW4 + "/_SUCCESS");
            fs.delete(pt, false);

            // Copy the result from HDFS to local.
            pt = new Path(WORK_DIR + operatorW1);
            fu.copy(fs, pt, new File(OUTPUT_DIR + "operatorW1"), false, conf);
            pt = new Path(WORK_DIR + operatorW2);
            fu.copy(fs, pt, new File(OUTPUT_DIR + "operatorW2"), false, conf);
            pt = new Path(WORK_DIR + operatorW3);
            fu.copy(fs, pt, new File(OUTPUT_DIR + "operatorW3"), false, conf);
            pt = new Path(WORK_DIR + operatorW4);
            fu.copy(fs, pt, new File(OUTPUT_DIR + "operatorW4"), false, conf);
            pt = new Path(WORK_DIR + psi);
            fu.copy(fs, pt, new File(OUTPUT_DIR + "psi"), false, conf);


            // Delete the WORK_DIR directory.
            pt = new Path(WORK_DIR);
            fs.delete(pt, true);


            fs.close();

            System.out.println("Finished!");

            System.out.println("Time to generate the matrices = "
                    + ((System.nanoTime() - startTime) / Math.pow(10, 9))
                    + " seconds");

        } catch (Exception e) {
            System.out.println(e);
        }

    }
}
