/*
 * GS.java    1.1 2015/06/07
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


package gs;


import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.io.File;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.conf.Configuration;


/**
 *
    This software simulate the Grover's algorithm using Apache Hadoop.
 *
 * @version
    1.1 7 Jun 2015  * @author
    David Souza  */


public class GS {

    /**
     * This is the size of the list where the Grover algorithm will do the
     * search.
     */
    private static final int N = 4;

    /**
     * The path in the HDFS where the program will store the data.
     */
    private static final String PATH = "gs_tmp/";

    /**
     * The folder path where the .jar files are stored.
     */
    private static final String JAR_DIR = "/home/david/Desktop/java/GS/";

    /**
     * The folder path where the result will be stored.
     */
    private static final String OUTPUT_DIR =
            "/home/david/Desktop/java/GS/Result/";

    public static void main(String[] args) throws Exception {

        long startTime = System.nanoTime();
        long n = (long) Math.pow(2, N);
        int steps = (int) ((Math.PI / 4) * Math.sqrt(n));
        String psi;
        String psiT;
        String pdf;
        Runtime rt = Runtime.getRuntime();
        Process pr;
        BufferedWriter bw;
        File fl;

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        FileUtil fu = new FileUtil();
        Path pt;
        FileStatus[] status;

        try {
            // Delete the PATH directory if exists. And create a new one empty.
            pt = new Path(PATH);
            fs.delete(pt, true);
            fs.mkdirs(pt);

            // Start of psi generation
            psi = "psi";
            pt = new Path(PATH + "part-r-" + psi);
            bw = new BufferedWriter(new OutputStreamWriter(fs.create(pt,
                    true)));

            for (long i = 0; i < n; i++) {
                bw.write(Long.toString(i) + "," + Double.toString(1
                        / Math.sqrt(n)) + "\n");
            }
            bw.close();

            System.out.println("Time to generate psi = " + ((System.nanoTime()
                    - startTime) / Math.pow(10, 9)) + " seconds");    

            startTime = System.nanoTime();

            System.out.println("Executing the steps...");

            // End of psi generation. Start of the Steps

            psiT = "psiT";

            // Delete the input directory if exists and create a new one.
            pt = new Path(PATH + psiT + "_input");
            fs.delete(pt, true);
            fs.mkdirs(pt);

            fu.copy(fs, new Path(PATH + "part-r-" + psi), fs, pt, false, true,
                    conf);

            for (int i = 0; i < steps; i++) {

                if (i > 0) {

                    pt = new Path(PATH + psiT + "_input");
                    status = fs.listStatus(pt);
                    for (FileStatus stat : status) {

                        if (stat.getPath().toString().indexOf("part-") > -1) {
                            fs.delete(stat.getPath(), false);
                        }

                    }

                    status = fs.listStatus(new Path(PATH + psiT));
                    for (FileStatus stat : status) {

                        if (stat.getPath().toString().indexOf("part-") > -1) {
                            fs.rename(stat.getPath(), pt);
                        }

                    }

                    pt = new Path(PATH + psiT);
                    fs.delete(pt, true);

                }

                pr = rt.exec("hadoop jar " + JAR_DIR
                        + "grover.jar grover.Grover " + PATH + psiT + "_input"
                        + " " + PATH + psiT + " " + Integer.toString(N));

                pr.waitFor();
                pr.destroy();

                System.out.println("End of the Step " + (i + 1));
            }

            // Merge psiT output files.
            fu.copyMerge(fs, new Path(PATH + psiT), fs, new Path(PATH + psiT
                    + "_" + Integer.toString(N) + "/part-0"), true, conf, null);


            // Calculate the probability distribution function
            pdf = "pdf";
            pr = rt.exec("hadoop jar " + JAR_DIR
                    + "grover.jar grover.PDF " + PATH + psiT + "_"
                    + Integer.toString(N) + " " + PATH + pdf);

            pr.waitFor();
            pr.destroy();

            System.out.println("End of the PDF calculation.");

            // Delete the OUTPUT_DIR if it exists.
            fl = new File(OUTPUT_DIR);
            fu.fullyDelete(fl);

            // Copy the result from HDFS to local.
            pt = new Path(PATH + psiT + "_" + Integer.toString(N));
            fl = new File(OUTPUT_DIR + psiT + "_" + Integer.toString(N));
            fu.copy(fs, pt, fl, false, conf);

            pt = new Path(PATH + pdf);
            fl = new File(OUTPUT_DIR + pdf);
            fu.copy(fs, pt, fl, false, conf);


            // Create a chart of the probability distribution function of psiT
            pr = rt.exec("hadoop jar " + JAR_DIR
                    + "grover.jar grover.PDFChart " + OUTPUT_DIR + pdf
                    + "/part-r-00000" + " " + OUTPUT_DIR);

            pr.waitFor();
            pr.destroy();

            System.out.println("PNG chart created.");


            // Delete the PATH directory.
            pt = new Path(PATH);
            fs.delete(pt, true);

            fs.close();


            System.out.println("Finished!");

            System.out.println("Steps Runtime = " + ((System.nanoTime()
                    - startTime) / Math.pow(10, 9)) + " seconds");

            // End of the Steps

        } catch (Exception e) {
            System.out.println(e);
        }

    }

}

