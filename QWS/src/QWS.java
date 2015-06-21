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
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.FileInputStream;
import java.util.Properties;
import java.io.IOException;

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

    public static void main(String[] args) throws Exception {

        long startTime;
        String steps;
        String paths;
        String workDir;
        String jarDir;
        String newShape;
        String axes;
        String position;
        String outputDir;
        String line;
        String psi;
        String psiT;
        String psiTNorm;
        String absSquare;
        String reshape;
        String pdf;
        BufferedReader br;
        int numberU;
        String[] uDir;
        Runtime rt;
        Process pr;
        Properties prop = new Properties();
	    InputStream configInput = null;

        Configuration conf = new Configuration();
        FileSystem fs;
        FileUtil fu = new FileUtil();
        Path pt;
        FileStatus[] status;

        try {

            startTime = System.nanoTime();

            fs = FileSystem.get(conf);

            configInput = new FileInputStream("config.properties");

            // load the properties file
		    prop.load(configInput);

            // Get the configurations in the properties file
		    steps = prop.getProperty("steps");
		    paths = prop.getProperty("paths");
		    workDir = prop.getProperty("workDir");
		    jarDir = prop.getProperty("jarDir");
		    newShape = prop.getProperty("newShape");
		    axes = prop.getProperty("axes");
		    position = prop.getProperty("position");
		    outputDir = prop.getProperty("outputDir");

            if (steps == null || steps.equals("")) {
                throw new IOException ("The value of the configuration "
                        + "\"steps\" can not be null or empty.");
            }

            if (paths == null || paths.equals("")) {
                throw new IOException ("The value of the configuration "
                        + "\"paths\" can not be null or empty.");
            } 

            if (workDir == null || workDir.equals("")) {
                throw new IOException ("The value of the configuration "
                        + "\"workDir\" can not be null or empty.");
            }

            if (jarDir == null || jarDir.equals("")) {
                throw new IOException ("The value of the configuration "
                        + "\"jarDir\" can not be null or empty.");
            }

            if (outputDir == null || outputDir.equals("")) {
                throw new IOException ("The value of the configuration "
                        + "\"outputDir\" can not be null or empty.");
            }

            configInput.close();

            /*
             * Delete the workDir directory if exists. And create a new one
             * empty.
             */
            pt = new Path(workDir);
            fs.delete(pt, true);
            fs.mkdirs(pt);

            br = new BufferedReader(new FileReader(paths));

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

            br = new BufferedReader(new FileReader(paths));
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

            pt = new Path(workDir + "psi");
            fs.delete(pt, true);
            fs.copyFromLocalFile(new Path(psi), pt);

            for (int i = 0; i < numberU; i++) {
                pt = new Path(workDir + "u" + Integer.toString(i));
                fs.delete(pt, true);
                fs.copyFromLocalFile(new Path(uDir[i]), pt);
            }

            psiT = workDir + "psiT";
            pt = new Path(psiT);
            fs.mkdirs(pt);

            status = fs.listStatus(new Path(workDir + "psi"));
            for (FileStatus stat : status) {

                fu.copy(fs, stat.getPath(), fs, pt, false, true, conf);
            }

            for (int i = 0; i < numberU; i++) {
                pt = new Path(workDir + "u" + Integer.toString(i));
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

            for (int i = 0; i < Integer.parseInt(steps); i++) {

                for (int j = numberU - 1; j > -1; j--) {

                    pt = new Path(psiT + "_tmp");
                    fs.delete(pt, true);
                    fs.mkdirs(pt);

                    status = fs.listStatus(new Path(psiT));
                    for (FileStatus stat : status) {

                        if (stat.getPath().toString().indexOf("_logs") > -1) {
                            continue;
                        } else {
                            if (stat.getPath().toString().indexOf("_SUCCESS")
                                    > -1) {
                                continue;
                            } else {
                                fs.rename(stat.getPath(), pt);
                            }
                        }
                    }

                    pt = new Path(psiT);
                    fs.delete(pt, true);

                    pt = new Path(psiT + "_tmp");

                    status = fs.listStatus(new Path(workDir + "u"
                                + Integer.toString(j)));
                    for (FileStatus stat : status) {

                        fu.copy(fs, stat.getPath(), fs, pt, false, true, conf);
                    }

                    pr = rt.exec("hadoop jar " + jarDir
                            + "operations.jar operations.MultMatrix " + psiT
                            + "_tmp" + " " + workDir + "tmp" + " " + psiT
                            + " B");

                    pr.waitFor();

                    br = new BufferedReader(new InputStreamReader(
                            pr.getInputStream()));

                    if (pr.exitValue() != 0) {

                        while ((line = br.readLine()) != null) {
                            System.out.println(line);
                        }

                        System.exit(1);
                    }

                    pr.destroy();

                }

                System.out.println("End of the Step " + (i + 1));
            }


            psiTNorm = workDir + "psiTNorm";

            // Delete the output directory if exists.
            pt = new Path(psiTNorm);
            fs.delete(pt, true);

            pr = rt.exec("hadoop jar " + jarDir + "operations.jar operations."
                    + "NormMatrix " + psiT + " " + psiTNorm);

            pr.waitFor();

            br = new BufferedReader(new InputStreamReader(
                    pr.getInputStream()));

            if (pr.exitValue() != 0) {

                while ((line = br.readLine()) != null) {
                    System.out.println(line);
                }

                System.exit(1);
            }

            pr.destroy();

            System.out.println("End of the psiTNorm.");

            // Put the file with the header in the first position in the folder.
            pt = new Path(psiT);
            status = fs.listStatus(pt);
            for (FileStatus stat : status) {

                if (stat.getPath().toString().indexOf("part-") > -1) {
                    br = new BufferedReader(new InputStreamReader(fs.open(stat.
                            getPath())));

                    line = br.readLine();
                    if (line.indexOf("#") > -1) {
                        fs.rename(stat.getPath(), new Path(stat.getPath().
                                toString().replaceAll("part-", "-")));
                        break;
                    }
                }

            }

            // Merge psiT output files.
            pt = new Path(psiT);
            fu.copyMerge(fs, pt, fs, new Path(psiT + "_New/part-0"), true, conf,
                    null);
            fs.rename(new Path(psiT + "_New"), pt);

            // Start of the absSquare
            absSquare = workDir + "absSquare";

            // Delete the output directory if exists.
            pt = new Path(absSquare);
            fs.delete(pt, true);

            /*
             * Computes the square of the absolute value for each element of the
             * array.
             */
            pr = rt.exec("hadoop jar " + jarDir + "operations.jar operations."
                    + "AbsSquare " + psiT + " " + absSquare);

            pr.waitFor();

            br = new BufferedReader(new InputStreamReader(
                    pr.getInputStream()));

            if (pr.exitValue() != 0) {

                while ((line = br.readLine()) != null) {
                    System.out.println(line);
                }

                System.exit(1);
            }

            pr.destroy();

            // End of the absSquare

            // Start of the reshape
            reshape = workDir + "reshape";

            // Delete the output directory if exists.
            pt = new Path(reshape);
            fs.delete(pt, true);

            // Gives a new shape for the array.
            pr = rt.exec("hadoop jar " + jarDir + "operations.jar operations."
                    + "Reshape " + newShape + " " + absSquare + " " + reshape);

            pr.waitFor();

            br = new BufferedReader(new InputStreamReader(
                    pr.getInputStream()));

            if (pr.exitValue() != 0) {

                while ((line = br.readLine()) != null) {
                    System.out.println(line);
                }

                System.exit(1);
            }

            pr.destroy();

            // End of the reshape

            /*
             * Start of the sumAxis. In this case the output of SunAxis function
             * will be the PDF of psiT.
             */
            pdf = workDir + "pdf";

            // Delete the output directory if exists.
            pt = new Path(pdf);
            fs.delete(pt, true);

            /*
             * Sum the elements of the array over given axes, for a specific
             * position.
             */ 
            pr = rt.exec("hadoop jar " + jarDir + "operations.jar operations."
                    + "SumAxis " + axes + " " +  position + " " + reshape + " "
                    + pdf);

            pr.waitFor();

            br = new BufferedReader(new InputStreamReader(
                    pr.getInputStream()));

            if (pr.exitValue() != 0) {

                while ((line = br.readLine()) != null) {
                    System.out.println(line);
                }

                System.exit(1);
            }

            pr.destroy();

            // End of the sumAxis

            System.out.println("End of the pdf.");

            /*
             * Delete _logs folder and _SUCCESS file in the psiT and psiTNorm
             * folders.
             */
            pt = new Path(psiT + "/_logs");
            fs.delete(pt, true);
            pt = new Path(psiT + "/_SUCCESS");
            fs.delete(pt, false);
            pt = new Path(psiTNorm + "/_logs");
            fs.delete(pt, true);
            pt = new Path(psiTNorm + "/_SUCCESS");
            fs.delete(pt, false);
            pt = new Path(pdf + "/_logs");
            fs.delete(pt, true);
            pt = new Path(pdf + "/_SUCCESS");
            fs.delete(pt, false);

            // Delete the outputDir if it exists.
            fu.fullyDelete(new File(outputDir));

            // Copy the result from HDFS to local.
            pt = new Path(psiT);
            fu.copy(fs, pt, new File(outputDir + "psiT"), false, conf);
            pt = new Path(psiTNorm);
            fu.copy(fs, pt, new File(outputDir + "psiTNorm"), false, conf);
            pt = new Path(pdf);
            fu.copy(fs, pt, new File(outputDir + "pdf"), false, conf);

            // Delete the workDir directory.
            pt = new Path(workDir);
            fs.delete(pt, true);

            fs.close();

            System.out.println("Finished!");

            System.out.println("Runtime = " + ((System.nanoTime() - startTime)
                    / Math.pow(10, 9)) + " seconds");

        } catch (NullPointerException e) {

            System.out.println("Some input path has a empty directory.");

        } catch (Exception e) {

            System.out.println(e);

        }

    }

}

