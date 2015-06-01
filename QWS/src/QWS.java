/*
 * QWS.java	1.0 2015/05/20
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

import java.lang.Runtime;
import java.lang.Process;
import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
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
	This software simulate a quantum walk or other problem that can be
	solved with a sequence of matrices multiplication using Apache Hadoop
	.
 *
 * @version
	1.0 20 May 2015  * @author
	David Souza  */


public class QWS {

	// The number of steps that will be executed in the simulation.
	private static final int STEPS = 8;

	// The path of the file that contains the paths to the files of all matrices U and the vector psi. The order matter. Should be: U_0,U_1,...,U_n,psi. One per line.
	private static final String FILES = "/home/david/Desktop/java/QWS/input/files";

	// The path in the HDFS where the program will store the data.
	private static final String WORK_DIR = "qws_tmp/";

	// The folder path where the .jar files are stored.
	private static final String JAR_DIR = "/home/david/Desktop/java/QWS/";

	// The folder path where the result will be stored.
	private static final String OUTPUT_DIR = "/home/david/Desktop/java/QWS/Result/";


	public static void main(String[] args) throws Exception {

		long startTime;
		String line;
		String psi;
		String psi_t;
		String psi_t_norm;
		int number_u;
		String[] u_dir;
		Runtime rt;
		Process pr;

		Configuration conf = new Configuration();
		FileSystem fs;
		FileUtil fu;
		Path pt;
		FileStatus[] status;

		 try{

			startTime = System.nanoTime();
		
			fs = FileSystem.get(conf);
			fu = new FileUtil();

			// Delete the WORK_DIR directory if exists. And create a new one empty.
			pt = new Path(WORK_DIR);
			fs.delete(pt, true);
			fs.mkdirs(pt);

			BufferedReader br = new BufferedReader(new FileReader(FILES));

			number_u = -1;	 // The loop below computes the number of U passed in input and stores it in this variable.
			while ((line = br.readLine()) != null) {

				if (!(line.equals("")) && !(String.valueOf(line.charAt(0)).equals(" "))){
					//System.out.println(line);
					number_u++;
				}
			}

			br.close();

			//System.out.println("number_u="+number_u);
			
			u_dir = new String[number_u];

			br = new BufferedReader(new FileReader(FILES));
			line = br.readLine();

			psi = "";
			for (int i=0; i<number_u+1; i++) {
				
				if (!(line.equals("")) && !(String.valueOf(line.charAt(0)).equals(" "))){

					if (line == null){
						break;
					}
				
					if (i < number_u) {
						u_dir[i] = line;
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

			//System.out.println("psi="+psi);

			pt = new Path(WORK_DIR + "psi");
			fs.delete(pt, true);
			fs.copyFromLocalFile(new Path(psi), pt);

			for (int i=0; i<number_u; i++){
				pt = new Path(WORK_DIR + "u" + Integer.toString(i));
				fs.delete(pt, true);
				fs.copyFromLocalFile(new Path(u_dir[i]), pt);
			}

			psi_t = WORK_DIR + "psi_t";
			pt = new Path(psi_t);
			fs.mkdirs(pt);

			status = fs.listStatus(new Path(WORK_DIR + "psi"));
			for (FileStatus stat : status) {

				fu.copy(fs, stat.getPath(), fs, pt, false, true, conf);
			}

			for (int i=0; i<number_u; i++){
				pt = new Path(WORK_DIR + "u" + Integer.toString(i));
				status = fs.listStatus(pt);
				for (FileStatus stat : status) {

					fs.rename(stat.getPath(), new Path(stat.getPath().toString().
						replaceAll("part-r-", "part-U" + Integer.toString(i) + "-")));
				}
				
			}

			System.out.println("The preparation of the files is complete.\nExecuting the steps...");

			rt = Runtime.getRuntime();
			
			for (int i = 0; i < STEPS; i++) {

				for (int j = number_u - 1; j > -1; j--) {

					pt = new Path(psi_t + "_tmp");
					fs.delete(pt, true);
					fs.mkdirs(pt);

					status = fs.listStatus(new Path(psi_t));
					for (FileStatus stat : status) {
							
						if (stat.getPath().toString().indexOf("_logs") > -1) {

							//fs.delete(stat.getPath(), true);
							continue;
						} else {

							if (stat.getPath().toString().indexOf("_SUCCESS") > -1) {

								//fs.delete(stat.getPath(), true);
								continue;
							} else {

								fs.rename(stat.getPath(), pt);
							}
						}
					}

					pt = new Path(psi_t);
					fs.delete(pt, true);

					pt = new Path(psi_t + "_tmp");

					status = fs.listStatus(new Path(WORK_DIR + "u" + Integer.toString(j)));
					for (FileStatus stat : status) {

						fu.copy(fs, stat.getPath(), fs, pt, false, true, conf);
					}					

					pr = rt.exec("hadoop jar " + JAR_DIR + "mult.jar mult.MultMatrix " + 
						psi_t + "_tmp" + " " + WORK_DIR + "tmp" + " " +
						psi_t + " B");
		

					pr.waitFor();
					pr.destroy();

				}

				System.out.println("End of the Step "+(i+1));
			}


			psi_t_norm = WORK_DIR + "psi_t_norm";

			// Delete the output directory if exists.
			pt = new Path(psi_t_norm);
			fs.delete(pt, true);
			
			pr = rt.exec("hadoop jar " + JAR_DIR + "mult.jar mult.NormMatrix " + 
				psi_t + " " + psi_t_norm);			

			pr.waitFor();
			pr.destroy();

			System.out.println("End of the psi_t_norm.");	


			// Delete the OUTPUT_DIR if it exists.
			fu.fullyDelete(new File(OUTPUT_DIR));

			// Copy the result from HDFS to local.
			pt = new Path(psi_t);
			fs.copyToLocalFile(pt, new Path(OUTPUT_DIR + "psi_t"));
			pt = new Path(psi_t_norm);
			fs.copyToLocalFile(pt, new Path(OUTPUT_DIR + "psi_t_norm"));


			// Delete the WORK_DIR directory.
			pt = new Path(WORK_DIR);
			fs.delete(pt, true);

			fs.close();

			System.out.println("Finished!");
			
			System.out.println("Runtime = " + ((System.nanoTime() - startTime) /
				Math.pow(10, 9)) + " seconds");

                }catch(Exception e){
                        System.out.println(e);
                }

	}

}


