/*
 * QW.java    1.0 2015/04/03
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


package qw;

import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.io.File;
import java.io.BufferedReader;
import java.io.InputStreamReader;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.conf.Configuration;


/**
 *
    This software simulate a quantum walk with two particles in a two
    dimensional lattice using Apache Hadoop.
 *
 * @version
    1.0 3 Apr 2015  * @author
    David Souza  */


public class QW {

    /**
     * The dimensions of the two dimensional lattice.
     */
    private static final int SIZE = 5;

    /**
     * The number of steps that will be executed in the simulation.
     */
    private static final int STEPS = 8;

    /**
     * The path in the HDFS where the program will store the data.
     */
    private static final String WORK_DIR = "qw_tmp/";

    /**
     * The folder path where the .jar files are stored.
     */
    private static final String JAR_DIR = "/home/david/Desktop/java/QW/";

    /**
     * The folder path where the result will be stored. Should be a empty folder
     * because this ALL DATA will be deleted.
     */
    private static final String OUTPUT_DIR =
            "/home/david/Desktop/java/QW/Result/";

    /*
     * Set for true to delete unnecessary files during the execution to increase
     * available storage space.
     */
    private static final boolean CLEAN_FOLDERS = true;


    public static void main(String[] args) throws Exception {

        long startTime;
        long walkersStateCount;
        int cfJ;
        int cfK;
        String hadamardA;
        String hadamardB;
        String hadamard;
        String identity;
        String operatorCoinW1;
        String operatorShiftW1;
        String operatorW1A;
        String operatorW1B;
        String identityW2A;
        String operatorW2A;
        String identityW2B;
        String operatorW2B;
        String operatorG;
        String operatorW2;
        String operatorW;
        String walkersState;
        String walkersStateT;
        String line;
        String absSquare;
        String reshape;
        String pdf;
        String walkersStateNorm;
        BufferedWriter bw;
        BufferedReader br;
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
            rt = Runtime.getRuntime();

            System.out.println("The matrices are being generated...");

            /*
             * Delete the WORK_DIR directory if exists. And create a new one
             * empty.
             */
            pt = new Path(WORK_DIR);
            fs.delete(pt, true);
            fs.mkdirs(pt);

            // Start of the hadamard
            hadamardA = "hadamardA";
            pt = new Path(WORK_DIR + hadamardA);
            bw = new BufferedWriter(new OutputStreamWriter(fs.create(pt,
                    true)));
            bw.write("#A,2,2\n");
            bw.write("A,0,0," + Double.toString(1.0 / Math.sqrt(2)) + "j0\n");
            bw.write("A,0,1," + Double.toString(1.0 / Math.sqrt(2)) + "j0\n");
            bw.write("A,1,0," + Double.toString(1.0 / Math.sqrt(2)) + "j0\n");
            bw.write("A,1,1," + Double.toString(-1.0 / Math.sqrt(2)) + "j0");
            bw.close();

            hadamardB = "hadamardB";
            pt = new Path(WORK_DIR + hadamardB);
            bw = new BufferedWriter(new OutputStreamWriter(fs.create(pt,
                    true)));
            bw.write("#B,2,2\n");
            bw.write("B,0,0," + Double.toString(1.0 / Math.sqrt(2)) + "j0\n");
            bw.write("B,0,1," + Double.toString(1.0 / Math.sqrt(2)) + "j0\n");
            bw.write("B,1,0," + Double.toString(1.0 / Math.sqrt(2)) + "j0\n");
            bw.write("B,1,1," + Double.toString(-1.0 / Math.sqrt(2)) + "j0");
            bw.close();

            hadamard = "hadamard";

            // Delete the output directory if exists.
            pt = new Path(WORK_DIR + hadamard);
            fs.delete(pt, true);

            // Delete the input directory if exists and create a new one.
            pt = new Path(WORK_DIR + hadamard + "_input");
            fs.delete(pt, true);
            fs.mkdirs(pt);

            // Move hadamardA and hadamardB to hadamard_input
            fs.rename(new Path(WORK_DIR + hadamardA), new Path(WORK_DIR
                    + hadamard + "_input"));
            fs.rename(new Path(WORK_DIR + hadamardB), new Path(WORK_DIR
                    + hadamard + "_input"));

            pr = rt.exec("hadoop jar " + JAR_DIR + "operations.jar operations."
                    + "KronMatrix " + WORK_DIR + hadamard + "_input" + " "
                    + WORK_DIR + hadamard + " A");

            pr.waitFor();
            pr.destroy();

            if (CLEAN_FOLDERS) {
                fs.delete(pt, true);
            }

            System.out.println("End of the hadamard.");

            // End of the hadamard. Start of the operatorCoinW1

            identity = "identity";
            pt = new Path(WORK_DIR + identity);
            bw = new BufferedWriter(new OutputStreamWriter(fs.create(pt,
                    true)));
            bw.write("#B," + Integer.toString(SIZE * SIZE) + ","
                    + Integer.toString(SIZE * SIZE));

            for (int i = 0; i < SIZE * SIZE; i++) {
                bw.write("\nB," + Integer.toString(i) + ","
                        + Integer.toString(i) + ",1.0j0");
            }
            bw.close();


            operatorCoinW1 = "operatorCoinW1";

            // Delete the output directory if exists.
            pt = new Path(WORK_DIR + operatorCoinW1);
            fs.delete(pt, true);

            // Delete the input directory if exists and create a new one.
            pt = new Path(WORK_DIR + operatorCoinW1 + "_input");
            fs.delete(pt, true);
            fs.mkdirs(pt);


            fs.rename(new Path(WORK_DIR + identity), new Path(WORK_DIR
                    + operatorCoinW1 + "_input"));

            status = fs.listStatus(new Path(WORK_DIR + hadamard));
            for (FileStatus stat : status) {

                if (stat.getPath().toString().indexOf("part-") > -1) {

                    fs.rename(stat.getPath(), new Path(WORK_DIR + operatorCoinW1
                            + "_input"));
                }

            }

            pr = rt.exec("hadoop jar " + JAR_DIR + "operations.jar operations."
                    + "KronMatrix " + WORK_DIR + operatorCoinW1 + "_input" + " "
                    + WORK_DIR + operatorCoinW1 + " B");

            pr.waitFor();
            pr.destroy();

            if (CLEAN_FOLDERS) {
                fs.delete(pt, true);
                pt = new Path(WORK_DIR + hadamard);
                fs.delete(pt, true);
            }

            System.out.println("End of the operatorCoinW1.");


            /*
             * End of the operatorCoinW1. Start of the operatorW1A and
             * operatorW1B.
             */

            operatorShiftW1 = "operatorShiftW1";
            pt = new Path(WORK_DIR + operatorShiftW1);
            bw = new BufferedWriter(new OutputStreamWriter(fs.create(pt,
                    true)));
            bw.write("#A," + Long.toString((long) (4 * Math.pow(SIZE, 2)))
                    + "," + Long.toString((long) (4 * Math.pow(SIZE, 2))));

            /*
             * Mod function for negative numbers in Java is different of the mod
             * function in Mathematics. Need a correction factor.
             */
            cfJ = 0;
            cfK = 0;

            for (int j = 0; j < 2; j++) {
                for (int k = 0; k < 2; k++) {
                    for (int x = 0; x < SIZE; x++) {
                        for (int y = 0; y < SIZE; y++) {

                            if (x + Math.pow(-1, j) < 0) {
                                cfJ = SIZE;
                            } else {
                                cfJ = 0;
                            }

                            if (y + Math.pow(-1, k) < 0) {
                                cfK = SIZE;
                            } else {
                                cfK = 0;
                            }


                            bw.write("\nA," + Long.toString((long) (((j * 2 + k)
                                    * SIZE + x) * SIZE + y)) + ","
                                    + Long.toString((long) ((((((j * 2) + k)
                                    * SIZE) + (cfJ + (x + Math.pow(-1, j))
                                    % SIZE)) * SIZE) + (cfK + (y
                                    + Math.pow(-1, k)) % SIZE))) + ",1.0j0");

                        }
                    }
                }
            }
            bw.close();


            operatorW1A = "operatorW1A";

            // Delete the output directory if exists.
            pt = new Path(WORK_DIR + operatorW1A);
            fs.delete(pt, true);

            // Delete the input directory if exists and create a new one.
            pt = new Path(WORK_DIR + operatorW1A + "_input");
            fs.delete(pt, true);
            fs.mkdirs(pt);


            fs.rename(new Path(WORK_DIR + operatorShiftW1), new Path(WORK_DIR
                    + operatorW1A + "_input"));

            status = fs.listStatus(new Path(WORK_DIR + operatorCoinW1));
            for (FileStatus stat : status) {

                if (stat.getPath().toString().indexOf("part-") > -1) {

                    fs.rename(stat.getPath(), new Path(WORK_DIR + operatorW1A
                            + "_input"));
                }

            }

            pr = rt.exec("hadoop jar " + JAR_DIR + "operations.jar operations."
                    + "MultMatrix " + WORK_DIR + operatorW1A + "_input" + " "
                    + WORK_DIR + "tmp" + " " + WORK_DIR + operatorW1A + " A");

            pr.waitFor();
            pr.destroy();

            System.out.println("End of the operatorW1A.");


            operatorW1B = "operatorW1B";

            // Delete the output directory if exists.
            pt = new Path(WORK_DIR + operatorW1B);
            fs.delete(pt, true);

            pr = rt.exec("hadoop jar " + JAR_DIR + "operations.jar operations."
                    + "MultMatrix " + WORK_DIR + operatorW1A + "_input" + " "
                    + WORK_DIR + "tmp" + " " + WORK_DIR + operatorW1B + " B");

            pr.waitFor();
            pr.destroy();

            if (CLEAN_FOLDERS) {
                pt = new Path(WORK_DIR + operatorCoinW1);
                fs.delete(pt, true);
                pt = new Path(WORK_DIR + operatorW1A + "_input");
                fs.delete(pt, true);
            }

            System.out.println("End of the operatorW1B.");

            /*
             * End of the operatorW1A and operatorW1B. Start of the operatorW2A
             * and operatorW2B.
             */

            identityW2A = "identityW2A";

            pt = new Path(WORK_DIR + identityW2A);
            bw = new BufferedWriter(new OutputStreamWriter(fs.create(pt,
                    true)));
            bw.write("#B," + Integer.toString(4 * SIZE * SIZE) + ","
                    + Integer.toString(4 * SIZE * SIZE));

            for (int i = 0; i < 4 * SIZE * SIZE; i++) {
                bw.write("\nB," + Integer.toString(i) + ","
                        + Integer.toString(i) + ",1.0j0");
            }
            bw.close();

            operatorW2A = "operatorW2A";

            // Delete the output directory if exists.
            pt = new Path(WORK_DIR + operatorW2A);
            fs.delete(pt, true);

            // Delete the input directory if exists and create a new one.
            pt = new Path(WORK_DIR + operatorW2A + "_input");
            fs.delete(pt, true);
            fs.mkdirs(pt);


            fs.rename(new Path(WORK_DIR + identityW2A), new Path(WORK_DIR
                    + operatorW2A + "_input"));

            status = fs.listStatus(new Path(WORK_DIR + operatorW1A));
            for (FileStatus stat : status) {

                if (stat.getPath().toString().indexOf("part-") > -1) {

                    fs.rename(stat.getPath(), new Path(WORK_DIR + operatorW2A
                            + "_input"));
                }

            }

            pr = rt.exec("hadoop jar " + JAR_DIR + "operations.jar operations."
                    + "KronMatrix " + WORK_DIR + operatorW2A + "_input" + " "
                    + WORK_DIR + operatorW2A + " A");

            pr.waitFor();
            pr.destroy();

            if (CLEAN_FOLDERS) {
                fs.delete(pt, true);
                pt = new Path(WORK_DIR + operatorW1A);
                fs.delete(pt, true);
            }

            System.out.println("End of the operatorW2A.");


            identityW2B = "identityW2B";

            pt = new Path(WORK_DIR + identityW2B);
            bw = new BufferedWriter(new OutputStreamWriter(fs.create(pt,
                    true)));
            bw.write("#A," + Integer.toString(4 * SIZE * SIZE) + ","
                    + Integer.toString(4 * SIZE * SIZE));

            for (int i = 0; i < 4 * SIZE * SIZE; i++) {
                bw.write("\nA," + Integer.toString(i) + ","
                        + Integer.toString(i) + ",1.0j0");
            }
            bw.close();

            operatorW2B = "operatorW2B";

            // Delete the output directory if exists.
            pt = new Path(WORK_DIR + operatorW2B);
            fs.delete(pt, true);

            // Delete the input directory if exists and create a new one.
            pt = new Path(WORK_DIR + operatorW2B + "_input");
            fs.delete(pt, true);
            fs.mkdirs(pt);


            fs.rename(new Path(WORK_DIR + identityW2B), new Path(WORK_DIR
                    + operatorW2B + "_input"));

            status = fs.listStatus(new Path(WORK_DIR + operatorW1B));
            for (FileStatus stat : status) {

                if (stat.getPath().toString().indexOf("part-") > -1) {

                    fs.rename(stat.getPath(), new Path(WORK_DIR + operatorW2B
                            + "_input"));
                }

            }

            pr = rt.exec("hadoop jar " + JAR_DIR + "operations.jar operations."
                    + "KronMatrix " + WORK_DIR + operatorW2B + "_input" + " "
                    + WORK_DIR + operatorW2B + " B");

            pr.waitFor();
            pr.destroy();

            if (CLEAN_FOLDERS) {
                fs.delete(pt, true);
                pt = new Path(WORK_DIR + operatorW1B);
                fs.delete(pt, true);
            }

            System.out.println("End of the operatorW2B.");

            // End of the operatorW2A and operatorW2B. Start of the operatorG

            operatorG = "operatorG";
            pt = new Path(WORK_DIR + operatorG);
            bw = new BufferedWriter(new OutputStreamWriter(fs.create(pt,
                    true)));
            bw.write("#B," + Long.toString((long) (16 * Math.pow(SIZE, 4)))
                    + "," + Long.toString((long) (16 * Math.pow(SIZE, 4))));

            for (long i = 0; i < (long) (16 * Math.pow(SIZE, 4)); i++) {
                bw.write("\nB," + Long.toString(i) + "," + Long.toString(i)
                        + ",1.0j0");
            }

            bw.close();

            System.out.println("End of the operatorG.");

            // End of the operatorG. Start of the operator W

            operatorW2 = "operatorW2";

            // Delete the output directory if exists.
            pt = new Path(WORK_DIR + operatorW2);
            fs.delete(pt, true);

            // Delete the input directory if exists and create a new one.
            pt = new Path(WORK_DIR + operatorW2 + "_input");
            fs.delete(pt, true);
            fs.mkdirs(pt);

            // Rename operatorW2A files
            status = fs.listStatus(new Path(WORK_DIR + operatorW2A));
            for (FileStatus stat : status) {

                if (stat.getPath().toString().indexOf("part-") > -1) {

                    fs.rename(stat.getPath(), new Path(
                            stat.getPath().toString() + "_A"));
                }

            }

            status = fs.listStatus(new Path(WORK_DIR + operatorW2A));
            for (FileStatus stat : status) {

                if (stat.getPath().toString().indexOf("part-") > -1) {

                    fs.rename(stat.getPath(), new Path(WORK_DIR + operatorW2
                            + "_input"));
                }

            }

            status = fs.listStatus(new Path(WORK_DIR + operatorW2B));
            for (FileStatus stat : status) {

                if (stat.getPath().toString().indexOf("part-") > -1) {

                    fs.rename(stat.getPath(), new Path(WORK_DIR + operatorW2
                            + "_input"));
                }

            }

            pr = rt.exec("hadoop jar " + JAR_DIR + "operations.jar operations."
                    + "MultMatrix " + WORK_DIR + operatorW2 + "_input" + " "
                    + WORK_DIR + "tmp" + " " + WORK_DIR + operatorW2 + " A");

            pr.waitFor();
            pr.destroy();

            if (CLEAN_FOLDERS) {
                fs.delete(pt, true);
                pt = new Path(WORK_DIR + operatorW2A);
                fs.delete(pt, true);
                pt = new Path(WORK_DIR + operatorW2B);
                fs.delete(pt, true);
            }

            System.out.println("End of the operatorW2.");


            operatorW = "operatorW";

            // Delete the output directory if exists.
            pt = new Path(WORK_DIR + operatorW);
            fs.delete(pt, true);

            // Delete the input directory if exists and create a new one.
            pt = new Path(WORK_DIR + operatorW + "_input");
            fs.delete(pt, true);
            fs.mkdirs(pt);

            fs.rename(new Path(WORK_DIR + operatorG), new Path(WORK_DIR
                    + operatorW + "_input"));

            status = fs.listStatus(new Path(WORK_DIR + operatorW2));
            for (FileStatus stat : status) {

                if (stat.getPath().toString().indexOf("part-") > -1) {

                    fs.rename(stat.getPath(), new Path(WORK_DIR + operatorW
                            + "_input"));
                }

            }

            pr = rt.exec("hadoop jar " + JAR_DIR + "operations.jar operations."
                    + "MultMatrix " + WORK_DIR + operatorW + "_input" + " "
                    + WORK_DIR + "tmp" + " " + WORK_DIR + operatorW + " A");

            pr.waitFor();
            pr.destroy();

            if (CLEAN_FOLDERS) {
                fs.delete(pt, true);
                pt = new Path(WORK_DIR + operatorW2);
                fs.delete(pt, true);
            }


            System.out.println("End of the operatorW.");



            // End of the operatorW. Start of the walkersState

            walkersState = "walkersState";

            pt = new Path(WORK_DIR + walkersState);
            fs.delete(pt, true);
            fs.mkdirs(pt);
            pt = new Path(WORK_DIR + walkersState + "/part-r");

            bw = new BufferedWriter(new OutputStreamWriter(fs.create(pt,
                    true)));
            bw.write("#B," + Long.toString((long) (16 * Math.pow(SIZE, 4)))
                    + ",1");

            walkersStateCount = 0;
            for (int k1 = 0; k1 < 2; k1++) {
                for (int k2 = 0; k2 < 2; k2++) {
                    for (int k3 = 0; k3 < SIZE; k3++) {
                        for (int k4 = 0; k4 < SIZE; k4++) {
                            for (int k5 = 0; k5 < 2; k5++) {
                                for (int k6 = 0; k6 < 2; k6++) {
                                    for (int k7 = 0; k7 < SIZE; k7++) {
                                        for (int k8 = 0; k8 < SIZE; k8++) {

                                            if ((k1 == 0) && (k2 == 0)
                                                    && (k3 == (int)SIZE/2)
                                                    && (k4 == (int)SIZE/2)
                                                    && (k5 == 1) && (k6 == 1)
                                                    && (k7 == (int)SIZE/2)
                                                    && (k8 == (int)SIZE/2)) {

                                                bw.write("\nB,"
                                                        + Long.toString(
                                                        walkersStateCount)
                                                        + ",0,"
                                                        + Double.toString(1.0
                                                        / Math.sqrt(2)) + "j0");

                                            }

                                            if ((k1 == 1) && (k2 == 1)
                                                    && (k3 == (int)SIZE/2)
                                                    && (k4 == (int)SIZE/2)
                                                    && (k5 == 0) && (k6 == 0)
                                                    && (k7 == (int)SIZE/2)
                                                    && (k8 == (int)SIZE/2)) {

                                                bw.write("\nB,"
                                                        + Long.toString(
                                                        walkersStateCount)
                                                        + ",0,"
                                                        + Double.toString(-1.0
                                                        / Math.sqrt(2)) + "j0");

                                            }

                                            walkersStateCount++;

                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }

            bw.close();

            System.out.println("End of the walkersState.");

            walkersStateT = "walkersStateT";

            status = fs.listStatus(new Path(WORK_DIR + operatorW));
            for (int i = 0; i < status.length; i++) {

                if (status[i].getPath().toString().indexOf("part-") > -1) {

                    fs.rename(status[i].getPath(), new Path(status[i].getPath().
                            toString().replaceAll("part-r-", "part-A-")));
                }

            }

            // Delete the output directory if exists.
            pt = new Path(WORK_DIR + walkersStateT);
            fs.delete(pt, true);

            // Delete the input directory if exists and create a new one.
            pt = new Path(WORK_DIR + walkersStateT + "_input");
            fs.delete(pt, true);
            fs.mkdirs(pt);


            status = fs.listStatus(new Path(WORK_DIR + operatorW));
            for (FileStatus stat : status) {

                if (stat.getPath().toString().indexOf("part-") > -1) {
                    fs.rename(stat.getPath(), pt);
                }

            }

            status = fs.listStatus(new Path(WORK_DIR + walkersState));
            for (FileStatus stat : status) {

                if (stat.getPath().toString().indexOf("part-") > -1) {
                    fs.rename(stat.getPath(), pt);
                }

            }

            System.out.println("Time to generate the matrices = "
                    + ((System.nanoTime() - startTime) / Math.pow(10, 9))
                    + " seconds");

            startTime = System.nanoTime();

            System.out.println("The matrices generation is complete.\n"
                    + "Executing the steps...");

            for (int i = 0; i < STEPS; i++) {

                if (i > 0) {

                    pt = new Path(WORK_DIR + walkersStateT + "_input");
                    status = fs.listStatus(pt);
                    for (FileStatus stat : status) {

                        if (stat.getPath().toString().indexOf("part-r") > -1) {
                            fs.delete(stat.getPath(), false);
                        }

                    }

                    status = fs.listStatus(new Path(WORK_DIR + walkersStateT));
                    for (FileStatus stat : status) {

                        if (stat.getPath().toString().indexOf("part-") > -1) {
                            fs.rename(stat.getPath(), pt);
                        }

                    }

                    pt = new Path(WORK_DIR + walkersStateT);
                    fs.delete(pt, true);

                }

                pr = rt.exec("hadoop jar " + JAR_DIR
                        + "operations.jar operations.MultMatrix " + WORK_DIR
                        + walkersStateT + "_input" + " " + WORK_DIR + "tmp"
                        + " " + WORK_DIR + walkersStateT + " B");


                pr.waitFor();
                pr.destroy();

                System.out.println("End of the Step " + i);
            }

            if (CLEAN_FOLDERS) {
                pt = new Path(WORK_DIR + operatorW);
                fs.delete(pt, true);
                pt = new Path(WORK_DIR + walkersState);
                fs.delete(pt, true);
                pt = new Path(WORK_DIR + walkersStateT + "_input");
                fs.delete(pt, true);
            }

            System.out.println("End of the walkersStateT.");

            // End of the walkersState. Start of the walkersStateNorm

            // Put the file with the header first.
            pt = new Path(WORK_DIR + walkersStateT);
            status = fs.listStatus(pt);
            for (FileStatus stat : status) {

                if (stat.getPath().toString().indexOf("part-") > -1) {
                    br = new BufferedReader(new InputStreamReader(fs.open(stat.
                            getPath())));

                    line = br.readLine();
                    if (line != null && line.indexOf("#") > -1) {
                        fs.rename(stat.getPath(), new Path(stat.getPath().
                                toString().replaceAll("part-", "-")));
                        break;
                    }
                }

            }

            fs.rename(pt, new Path(WORK_DIR + walkersStateT + "splitted"));

            // Merge walkersStateT output files.
            fu.copyMerge(fs, new Path(WORK_DIR + walkersStateT + "splitted"),
                    fs, new Path(WORK_DIR + walkersStateT + "/part-0"), true,
                    conf, null);

            walkersStateNorm = "walkersStateNorm";

            // Delete the output directory if exists.
            pt = new Path(WORK_DIR + walkersStateNorm);
            fs.delete(pt, true);

            pr = rt.exec("hadoop jar " + JAR_DIR + "operations.jar operations."
                    + "NormMatrix " + WORK_DIR + walkersStateT + " " + WORK_DIR
                    + walkersStateNorm);

            pr.waitFor();
            pr.destroy();

            System.out.println("End of the walkersStateNorm.");

            // End of the walkersStateNorm

            // Start of the absSquare
            absSquare = "absSquare";

            // Delete the output directory if exists.
            pt = new Path(WORK_DIR + absSquare);
            fs.delete(pt, true);

            /*
             * Computes the square of the absolute value for each element of the
             * array.
             */
            pr = rt.exec("hadoop jar " + JAR_DIR + "operations.jar operations."
                    + "AbsSquare " + WORK_DIR + walkersStateT + " " + WORK_DIR
                    + absSquare);

            pr.waitFor();
            pr.destroy();

            // End of the absSquare

            // Start of the reshape
            reshape = "reshape";

            // Delete the output directory if exists.
            pt = new Path(WORK_DIR + reshape);
            fs.delete(pt, true);

            // Gives a new shape for the array.
            pr = rt.exec("hadoop jar " + JAR_DIR + "operations.jar operations."
                    + "Reshape 2,2," + Integer.toString(SIZE) + ","
                    + Integer.toString(SIZE) + ",2,2," + Integer.toString(SIZE)
                    + "," + Integer.toString(SIZE) + " " + WORK_DIR + absSquare
                    + " " + WORK_DIR + reshape);

            pr.waitFor();
            pr.destroy();

            // End of the reshape

            /*
             * Start of the sumAxis. In this case the output of SunAxis function
             * will be the PDF of walkersStateT when the particle 2 is in the
             * position (SIZE/2, SIZE/2)
             */
            pdf = "pdf";

            // Delete the output directory if exists.
            pt = new Path(WORK_DIR + pdf);
            fs.delete(pt, true);

            /*
             * Sum the elements of the array over given axes, for a specific
             * position.
             */ 
            pr = rt.exec("hadoop jar " + JAR_DIR + "operations.jar operations."
                    + "SumAxis 1,2,5,6 ?,?,?,?,?,?,"
                    + Integer.toString((int)(SIZE/2)) + ","
                    + Integer.toString((int)(SIZE/2)) + " " + WORK_DIR + reshape
                    + " " + WORK_DIR + pdf);

            pr.waitFor();
            pr.destroy();

            // End of the sumAxis

            System.out.println("End of the pdf.");

            // Delete the OUTPUT_DIR if it exists.
            fu.fullyDelete(new File(OUTPUT_DIR));

            /*
             * Delete _logs folder and _SUCCESS file in the walkersStateNorm and
             * pdf folders.
             */
            pt = new Path(WORK_DIR + walkersStateNorm + "/_logs");
            fs.delete(pt, true);
            pt = new Path(WORK_DIR + walkersStateNorm + "/_SUCCESS");
            fs.delete(pt, false);
            pt = new Path(WORK_DIR + pdf + "/_logs");
            fs.delete(pt, true);
            pt = new Path(WORK_DIR + pdf + "/_SUCCESS");
            fs.delete(pt, false);

            // Copy the result from HDFS to local.
            pt = new Path(WORK_DIR + walkersStateT);
            fu.copy(fs, pt, new File(OUTPUT_DIR + "walkersStateT"), false,
                    conf);
            pt = new Path(WORK_DIR + walkersStateNorm);
            fu.copy(fs, pt, new File(OUTPUT_DIR + "walkersStateNorm"), false,
                    conf);
            pt = new Path(WORK_DIR + pdf);
            fu.copy(fs, pt, new File(OUTPUT_DIR + "pdf"), false, conf);


            // Delete the WORK_DIR directory.
            pt = new Path(WORK_DIR);
            fs.delete(pt, true);


            fs.close();

            System.out.println("Finished!");

            System.out.println("Steps Runtime = " + ((System.nanoTime()
                    - startTime) / Math.pow(10, 9)) + " seconds");

        } catch (Exception e) {
            System.out.println(e);
        }

    }

}


