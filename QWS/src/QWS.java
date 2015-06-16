/*
 * QWS.java    1.0 2015/05/20
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


package qws;

import java.io.File;
import java.io.FileReader;
import java.io.BufferedReader;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.conf.Configuration;


/**
 *
    This software simulate a quantum walk or other problem that can be solved
    with a sequence of matrices multiplication using Apache Hadoop.
 *
 * @version
    1.0 20 May 2015  * @author
    David Souza  */


public class QWS {

    /**
     * The number of steps that will be executed in the simulation.
     */
    private static final int STEPS = 8;

    /**
     * The path of the file that contains the paths to the files of all matrices
     * U and the vector psi. The order matter. Should be: U_0,U_1,...,U_n,psi.
     * One per line.
     */
    private static final String FILES =
            "/home/david/Desktop/java/QWS/input/files";

    /**
     * The path in the HDFS where the program will store the data.
     */
    private static final String WORK_DIR = "qws_tmp/";

    /**
     * The folder path where the .jar files are stored.
     */
    private static final String JAR_DIR = "/home/david/Desktop/java/QWS/";

    /**
     * The folder path where the result will be stored. Should be a empty folder
	 * because this ALL DATA will be deleted.
     */
    private static final String OUTPUT_DIR =
            "/home/david/Desktop/java/QWS/Result/";


    public static void main(String[] args) throws Exception {

        long startTime;
        String line;
        String psi;
        String psiT;
        String psiTNorm;
        int numberU;
        String[] uDir;
        Runtime rt;
        Process pr;

        Configuration conf = new Configuration();
        FileSystem fs;
        FileUtil fu;
        Path pt;
        FileStatus[] status;

        try {

            startTime = System.nanoTime();

            fs = FileSystem.get(conf);
            fu = new FileUtil();

            /*
             * Delete the WORK_DIR directory if exists. And create a new one
             * empty.
             */
            pt = new Path(WORK_DIR);
            fs.delete(pt, true);
            fs.mkdirs(pt);

            BufferedReader br = new BufferedReader(new FileReader(FILES));

            /*
             * The loop below computes the number of U passed in input and
             * stores it in this variable.
             */
            numberU = -1;
            while ((line = br.readLine()) != null) {

                if (!(line.equals("")) && !(String.valueOf(line.charAt(0)).
                        equals(" "))) {
                    numberU++;
                }
            }

            br.close();

            uDir = new String[numberU];

            br = new BufferedReader(new FileReader(FILES));
            line = br.readLine();

            psi = "";
            for (int i = 0; i < numberU + 1; i++) {

                if (!(line.equals("")) && !(String.valueOf(line.charAt(0)).
                        equals(" "))) {

                    if (line == null) {
                        break;
                    }

                    if (i < numberU) {
                        uDir[i] = line;
                    } else {
                        psi = line;
                    }

                    line = br.readLine();
                } else {

                    line = br.readLine();
                    i--;
                }


            }

            br.close();

            pt = new Path(WORK_DIR + "psi");
            fs.delete(pt, true);
            fs.copyFromLocalFile(new Path(psi), pt);

            for (int i = 0; i < numberU; i++) {
                pt = new Path(WORK_DIR + "u" + Integer.toString(i));
                fs.delete(pt, true);
                fs.copyFromLocalFile(new Path(uDir[i]), pt);
            }

            psiT = WORK_DIR + "psiT";
            pt = new Path(psiT);
            fs.mkdirs(pt);

            status = fs.listStatus(new Path(WORK_DIR + "psi"));
            for (FileStatus stat : status) {

                fu.copy(fs, stat.getPath(), fs, pt, false, true, conf);
            }

            for (int i = 0; i < numberU; i++) {
                pt = new Path(WORK_DIR + "u" + Integer.toString(i));
                status = fs.listStatus(pt);
                for (FileStatus stat : status) {

                    fs.rename(stat.getPath(), new Path(stat.getPath().
                            toString().replaceAll("part-r-", "part-U"
                            + Integer.toString(i) + "-")));
                }

            }

            System.out.println("The files preparation is complete.\n"
                    + "Executing the steps...");

            rt = Runtime.getRuntime();

            for (int i = 0; i < STEPS; i++) {

                for (int j = numberU - 1; j > -1; j--) {

                    pt = new Path(psiT + "_tmp");
                    fs.delete(pt, true);
                    fs.mkdirs(pt);

                    status = fs.listStatus(new Path(psiT));
                    for (FileStatus stat : status) {

                        if (stat.getPath().toString().indexOf("_logs") > -1) {

                            //fs.delete(stat.getPath(), true);
                            continue;
                        } else {

                            if (stat.getPath().toString().indexOf("_SUCCESS")
                                    > -1) {

                                //fs.delete(stat.getPath(), true);
                                continue;
                            } else {

                                fs.rename(stat.getPath(), pt);
                            }
                        }
                    }

                    pt = new Path(psiT);
                    fs.delete(pt, true);

                    pt = new Path(psiT + "_tmp");

                    status = fs.listStatus(new Path(WORK_DIR + "u"
                                + Integer.toString(j)));
                    for (FileStatus stat : status) {

                        fu.copy(fs, stat.getPath(), fs, pt, false, true, conf);
                    }

                    pr = rt.exec("hadoop jar " + JAR_DIR
                            + "operations.jar operations.MultMatrix " + psiT
                            + "_tmp" + " " + WORK_DIR + "tmp" + " " + psiT
                            + " B");


                    pr.waitFor();
                    pr.destroy();

                }

                System.out.println("End of the Step " + (i + 1));
            }


            psiTNorm = WORK_DIR + "psiTNorm";

            // Delete the output directory if exists.
            pt = new Path(psiTNorm);
            fs.delete(pt, true);

            pr = rt.exec("hadoop jar " + JAR_DIR + "operations.jar operations."
                    + "NormMatrix " + psiT + " " + psiTNorm);

            pr.waitFor();
            pr.destroy();

            System.out.println("End of the psiTNorm.");


            // Merge psiT output files.
            pt = new Path(psiT);
            fu.copyMerge(fs, pt, fs, new Path(psiT + "_New/part-0"), true, conf,
                    null);
            fs.rename(new Path(psiT + "_New"), pt);

            // Delete the OUTPUT_DIR if it exists.
            fu.fullyDelete(new File(OUTPUT_DIR));

            // Copy the result from HDFS to local.
            pt = new Path(psiT);
            fu.copy(fs, pt, new File(OUTPUT_DIR + "psiT"), false, conf);
            pt = new Path(psiTNorm);
            fu.copy(fs, pt, new File(OUTPUT_DIR + "psiTNorm"), false, conf);


            // Delete the WORK_DIR directory.
            pt = new Path(WORK_DIR);
            fs.delete(pt, true);

            fs.close();


            System.out.println("Finished!");

            System.out.println("Runtime = " + ((System.nanoTime() - startTime)
                    / Math.pow(10, 9)) + " seconds");

        } catch (Exception e) {
            System.out.println(e);
        }

    }

}

