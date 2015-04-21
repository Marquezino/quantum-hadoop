package qw;

import java.lang.Runtime;
import java.lang.Process;
import java.io.*;
import java.util.*;
import java.net.*;


import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class QW {

	private static final int SIZE = 10;
	private static final int STEPS = 5;
	private static final String PATH = "/user/david/qw_tmp/";
	private static final String JAR_DIR = "/home/david/Desktop/java/";
	private static final boolean CLEAN_FOLDERS = true;

	public static void main(String[] args) throws Exception {

		 try{

			long startTime = System.nanoTime();
		
			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.get(conf);
			FileUtil fu = new FileUtil();

			// Delete the PATH directory if exists. And create a new one empty.
			Path pt = new Path(PATH);
			fs.delete(pt,true);
			fs.mkdirs(pt);

			// ----------------------------------------------------------------------------------------------------
			// Start of the hadamard
			String hadamard_A = "hadamard_A";
			pt = new Path(PATH+hadamard_A);
			BufferedWriter br = new BufferedWriter(new OutputStreamWriter(fs.create(pt,true)));
			br.write("#A,2,2\n");
			br.write("A,0,0," + Double.toString(1.0/Math.sqrt(2)) + "j0\n");
			br.write("A,0,1," + Double.toString(1.0/Math.sqrt(2)) + "j0\n");
			br.write("A,1,0," + Double.toString(1.0/Math.sqrt(2)) + "j0\n");
			br.write("A,1,1," + Double.toString(-1.0/Math.sqrt(2)) + "j0");
			br.close();

			String hadamard_B = "hadamard_B";
			pt = new Path(PATH+hadamard_B);
			br = new BufferedWriter(new OutputStreamWriter(fs.create(pt,true)));
			br.write("#B,2,2\n");
			br.write("B,0,0," + Double.toString(1.0/Math.sqrt(2)) + "j0\n");
			br.write("B,0,1," + Double.toString(1.0/Math.sqrt(2)) + "j0\n");
			br.write("B,1,0," + Double.toString(1.0/Math.sqrt(2)) + "j0\n");
			br.write("B,1,1," + Double.toString(-1.0/Math.sqrt(2)) + "j0");
			br.close();

			String hadamard = "hadamard";

			// Delete the output directory if exists.
			pt = new Path(PATH+hadamard);
			fs.delete(pt,true);

			// Delete the input directory if exists and create a new one.
			pt = new Path(PATH+hadamard+"_input");
			fs.delete(pt,true);
			fs.mkdirs(pt);

			// Move hadamard_A and hadamard_B to hadamard_input
			fs.rename(new Path(PATH+hadamard_A), new Path(PATH+hadamard+"_input"));
			fs.rename(new Path(PATH+hadamard_B), new Path(PATH+hadamard+"_input"));
			
			Runtime rt = Runtime.getRuntime();
			Process pr = rt.exec("hadoop jar " + JAR_DIR + "mult.jar mult.KronMatrix " + 
						PATH+hadamard+"_input" + " " + PATH+hadamard + " A");			

			pr.waitFor();
			pr.destroy();

			if (CLEAN_FOLDERS) {
				fs.delete(pt,true);
			}

			System.out.println("End of the hadamard.");

			// End of the hadamard
			// --------------------------------------------------------------------------------------------------
			// Start of the operator_coin_W1

			String identity = "identity";
			pt = new Path(PATH+identity);
			br = new BufferedWriter(new OutputStreamWriter(fs.create(pt,true)));
			br.write("#B," + Integer.toString(SIZE*SIZE) + "," + Integer.toString(SIZE*SIZE));
		
			for (int i=0; i<SIZE*SIZE; i++) {
				br.write("\nB," + Integer.toString(i) + "," + Integer.toString(i) + ",1.0j0");
			}
			br.close();


			String operator_coin_W1 = "operator_coin_W1";

			// Delete the output directory if exists.
			pt = new Path(PATH+operator_coin_W1);
			fs.delete(pt,true);

			// Delete the input directory if exists and create a new one.
			pt = new Path(PATH+operator_coin_W1+"_input");
			fs.delete(pt,true);
			fs.mkdirs(pt);


			fs.rename(new Path(PATH+identity), new Path(PATH+operator_coin_W1+"_input"));

			FileStatus[] status = fs.listStatus(new Path(PATH+hadamard));
			for (FileStatus stat : status){
				
				if (stat.getPath().toString().indexOf("part-") > -1) {

					fs.rename(stat.getPath(), new Path(PATH+operator_coin_W1+"_input"));
				}			

			}
			
			pr = rt.exec("hadoop jar " + JAR_DIR + "mult.jar mult.KronMatrix " + 
						PATH+operator_coin_W1+"_input" + " " + PATH+operator_coin_W1 + " B");			

			pr.waitFor();
			pr.destroy();

			if (CLEAN_FOLDERS) {
				fs.delete(pt,true);
				pt = new Path(PATH+hadamard);
				fs.delete(pt,true);
			}

			System.out.println("End of the operator_coin_W1.");


			// End of the operator_coin_W1
			// --------------------------------------------------------------------------------------------------
			// Start of the operator_W1_A and operator_W1_B

			String operator_shift_W1 = "operator_shift_W1";
			pt = new Path(PATH+operator_shift_W1);
			br = new BufferedWriter(new OutputStreamWriter(fs.create(pt,true)));
			br.write("#A," + Long.toString((long)(4*Math.pow(SIZE,2))) + "," + Long.toString((long)(4*Math.pow(SIZE,2))));

			// Mod function for negative numbers in Java is different of the mod function in Mathematics. Need a correction factor.
			int cf_j = 0, cf_k = 0;  

			for (int j=0; j<2; j++) {
				for (int k=0; k<2; k++) {
					for (int x=0; x<SIZE; x++) {
						for (int y=0; y<SIZE; y++) {
														
							if (x+Math.pow(-1,j) < 0) {
								cf_j = SIZE;
							} else {
								cf_j = 0;
							}

							if (y+Math.pow(-1,k) < 0) {
								cf_k = SIZE;
							} else {
								cf_k = 0;
							}
							

							br.write("\nA," + Long.toString((long)(((j*2+k)*SIZE+x)*SIZE+y)) + "," + 
								Long.toString((long)((((((j*2)+k)*SIZE)+
										(cf_j+(x+Math.pow(-1,j))%SIZE))
										*SIZE)+
										(cf_k+(y+Math.pow(-1,k))%SIZE))) + ",1.0j0");

						}
					}
				}				
			}
			br.close();


			String operator_W1_A = "operator_W1_A";

			// Delete the output directory if exists.
			pt = new Path(PATH+operator_W1_A);
			fs.delete(pt,true);

			// Delete the input directory if exists and create a new one.
			pt = new Path(PATH+operator_W1_A+"_input");
			fs.delete(pt,true);
			fs.mkdirs(pt);


			fs.rename(new Path(PATH+operator_shift_W1), new Path(PATH+operator_W1_A+"_input"));

			status = fs.listStatus(new Path(PATH+operator_coin_W1));
			for (FileStatus stat : status){
				
				if (stat.getPath().toString().indexOf("part-") > -1) {

					fs.rename(stat.getPath(), new Path(PATH+operator_W1_A+"_input"));
				}			

			}
			
			pr = rt.exec("hadoop jar " + JAR_DIR + "mult.jar mult.MultMatrix " + 
						PATH+operator_W1_A+"_input" + " " + PATH+"tmp" + " " +
						PATH+operator_W1_A + " A");			

			pr.waitFor();
			pr.destroy();

			System.out.println("End of the operator_W1_A.");


			String operator_W1_B = "operator_W1_B";

			// Delete the output directory if exists.
			pt = new Path(PATH+operator_W1_B);
			fs.delete(pt,true);
			
			pr = rt.exec("hadoop jar " + JAR_DIR + "mult.jar mult.MultMatrix " + 
						PATH+operator_W1_A+"_input" + " " + PATH+"tmp" + " " +
						PATH+operator_W1_B + " B");			

			pr.waitFor();
			pr.destroy();

			if (CLEAN_FOLDERS) {
				pt = new Path(PATH+operator_coin_W1);
				fs.delete(pt,true);
				pt = new Path(PATH+operator_W1_A+"_input");
				fs.delete(pt,true);
			}

			System.out.println("End of the operator_W1_B.");

			// End of the operator_W1_A and operator_W1_B
			// --------------------------------------------------------------------------------------------------
			// Start of the operator_W2A and operator_W2B

			String identity_W2A = "identity_W2A";

			pt = new Path(PATH+identity_W2A);
			br = new BufferedWriter(new OutputStreamWriter(fs.create(pt,true)));
			br.write("#B," + Integer.toString(4*SIZE*SIZE) + "," + Integer.toString(4*SIZE*SIZE));
		
			for (int i=0; i < 4*SIZE*SIZE; i++) {
				br.write("\nB," + Integer.toString(i) + "," + Integer.toString(i) + ",1.0j0");
			}
			br.close();

			String operator_W2A = "operator_W2A";

			// Delete the output directory if exists.
			pt = new Path(PATH+operator_W2A);
			fs.delete(pt,true);

			// Delete the input directory if exists and create a new one.
			pt = new Path(PATH+operator_W2A+"_input");
			fs.delete(pt,true);
			fs.mkdirs(pt);


			fs.rename(new Path(PATH+identity_W2A), new Path(PATH+operator_W2A+"_input"));

			status = fs.listStatus(new Path(PATH+operator_W1_A));
			for (FileStatus stat : status){
				
				if (stat.getPath().toString().indexOf("part-") > -1) {

					fs.rename(stat.getPath(), new Path(PATH+operator_W2A+"_input"));
				}			

			}
			
			pr = rt.exec("hadoop jar " + JAR_DIR + "mult.jar mult.KronMatrix " + 
						PATH+operator_W2A+"_input" + " " + PATH+operator_W2A + " A");			

			pr.waitFor();
			pr.destroy();			

			if (CLEAN_FOLDERS) {
				fs.delete(pt,true);
				pt = new Path(PATH+operator_W1_A);
				fs.delete(pt,true);
			}

			System.out.println("End of the operator_W2A.");


			String identity_W2B = "identity_W2B";

			pt = new Path(PATH+identity_W2B);
			br = new BufferedWriter(new OutputStreamWriter(fs.create(pt,true)));
			br.write("#A," + Integer.toString(4*SIZE*SIZE) + "," + Integer.toString(4*SIZE*SIZE));
		
			for (int i=0; i < 4*SIZE*SIZE; i++) {
				br.write("\nA," + Integer.toString(i) + "," + Integer.toString(i) + ",1.0j0");
			}
			br.close();

			String operator_W2B = "operator_W2B";

			// Delete the output directory if exists.
			pt = new Path(PATH+operator_W2B);
			fs.delete(pt,true);

			// Delete the input directory if exists and create a new one.
			pt = new Path(PATH+operator_W2B+"_input");
			fs.delete(pt,true);
			fs.mkdirs(pt);


			fs.rename(new Path(PATH+identity_W2B), new Path(PATH+operator_W2B+"_input"));

			status = fs.listStatus(new Path(PATH+operator_W1_B));
			for (FileStatus stat : status){
				
				if (stat.getPath().toString().indexOf("part-") > -1) {

					fs.rename(stat.getPath(), new Path(PATH+operator_W2B+"_input"));
				}			

			}
			
			pr = rt.exec("hadoop jar " + JAR_DIR + "mult.jar mult.KronMatrix " + 
						PATH+operator_W2B+"_input" + " " + PATH+operator_W2B + " A");			

			pr.waitFor();
			pr.destroy();

			if (CLEAN_FOLDERS) {
				fs.delete(pt,true);
				pt = new Path(PATH+operator_W1_B);
				fs.delete(pt,true);
			}

			System.out.println("End of the operator_W2B.");

			// End of the operator_W2A and operator_W2B
			// --------------------------------------------------------------------------------------------------
			// Start of the operator_G

			String operator_G = "operator_G";
			pt = new Path(PATH+operator_G);
			br = new BufferedWriter(new OutputStreamWriter(fs.create(pt,true)));
			br.write("#A," + Long.toString((long)(16*Math.pow(SIZE,4))) + "," + Long.toString((long)(16*Math.pow(SIZE,4))));
			
			for (long i=0; i< (long)(16*Math.pow(SIZE,4)); i++) {
				br.write("\nA," + Long.toString(i) + "," + Long.toString(i) + ",1.0j0");
			}

			// Only if value in br.write != 1.0j0
			//for (int j1=0; j1<2; j1++) {
			//	for (int k1=0; k1<2; k1++) {
			//		for (int j2=0; j2<2; j2++) {
			//			for (int k2=0; k2<2; k2++) {
			//				for (int x=0; x<SIZE; x++) {
			//					for (int y=0; y<SIZE; y++) {
			//						
			//						long line = ((((((j1*2 + k1)*SIZE + x) * SIZE + y) * 2 + 
			//								j2) * 2 + k2) * SIZE + x) * SIZE + y;
			//
			//						br.write("\nB," + Long.toString(line) + "," +
			//								Long.toString(line) + ",1.0j0");
			//					}
			//				}
			//			}
			//		}
			//	}				
			//}
			br.close();

			System.out.println("End of the operator_G.");			

			// End of the operator_G
			// --------------------------------------------------------------------------------------------------
			// Start of the walkers_state

			String walkers_state = "walkers_state";

			pt = new Path(PATH+walkers_state);
			fs.delete(pt,true);
			fs.mkdirs(pt);
			pt = new Path(PATH+walkers_state+"/part-r");

			br = new BufferedWriter(new OutputStreamWriter(fs.create(pt,true)));
			br.write("#B," + Long.toString((long)(16*Math.pow(SIZE,4))) + ",1");
			
			long walkers_state_count = 0;
			for (int k1=0; k1<2; k1++) {
				for (int k2=0; k2<2; k2++) {
					for (int k3=0; k3<SIZE; k3++) {
						for (int k4=0; k4<SIZE; k4++) {
							for (int k5=0; k5<2; k5++) {
								for (int k6=0; k6<2; k6++) {
									for (int k7=0; k7<SIZE; k7++) {
										for (int k8=0; k8<SIZE; k8++) {

											if (k1==0 && k2==0 && k3==2 && k4==2 && k5==1 && k6==1 && k7==2 && k8==2) {
												
												br.write("\nB," + Long.toString(walkers_state_count) +
													",0," + Double.toString(1.0/Math.sqrt(2)) + "j0");

											}

											if (k1==1 && k2==1 && k3==2 && k4==2 && k5==0 && k6==0 && k7==2 && k8==2) {
												
												br.write("\nB," + Long.toString(walkers_state_count) +
													",0," + Double.toString(-1.0/Math.sqrt(2)) + "j0");

											}

											walkers_state_count++;

										}
									}
								}
							}
						}
					}
				}				
			}

			br.close();

			System.out.println("End of the walkers_state.");	

			String walkers_state_t = "walkers_state_t";

			// Delete the output directory if exists.
			pt = new Path(PATH+walkers_state_t);
			fs.delete(pt,true);

			// Delete the input directory if exists and create a new one.
			pt = new Path(PATH+walkers_state_t+"_input");
			fs.delete(pt,true);
			fs.mkdirs(pt);

			status = fs.listStatus(new Path(PATH+walkers_state));
			for (FileStatus stat : status) {

				if (stat.getPath().toString().indexOf("part-") > -1) {
					//fs.rename(stat.getPath(), pt);
					fu.copy(fs, stat.getPath(), fs, pt, false, true, conf);
				}

			}

			fs.rename(new Path(PATH+operator_G), pt);

			status = fs.listStatus(new Path(PATH+operator_W2A));
			for (int i = 0; i < status.length; i++){
				
				if (status[i].getPath().toString().indexOf("part-r") > -1) {

					fs.rename(status[i].getPath(), new Path(status[i].getPath().toString().replaceAll("part-r-","part-W2A-")));
				}

				if (status[i].getPath().toString().indexOf("_logs") > -1) {

					fs.delete(status[i].getPath(),true);
				}

				if (status[i].getPath().toString().indexOf("_SUCCESS") > -1) {

					fs.delete(status[i].getPath(),true);
				}			

			}

			status = fs.listStatus(new Path(PATH+operator_W2B));
			for (int i = 0; i < status.length; i++){
				
				if (status[i].getPath().toString().indexOf("part-r") > -1) {

					fs.rename(status[i].getPath(), new Path(status[i].getPath().toString().replaceAll("part-r-","part-W2B-")));
				}

				if (status[i].getPath().toString().indexOf("_logs") > -1) {

					fs.delete(status[i].getPath(),true);
				}

				if (status[i].getPath().toString().indexOf("_SUCCESS") > -1) {

					fs.delete(status[i].getPath(),true);
				}			

			}

			System.out.println("Time to generate the matrices = "+((System.nanoTime() - startTime)/Math.pow(10,9))+" seconds");	

			startTime = System.nanoTime();

			String G_walkers = "G_walkers";
			String W2B_G_walkers = "W2B_G_walkers";

			for (int i=0; i<STEPS; i++) {

				if (i > 0) {

					pt = new Path(PATH+walkers_state_t+"_input");
					status = fs.listStatus(pt);
					for (FileStatus stat : status) {

						if (stat.getPath().toString().indexOf("part-r") > -1) {
							fs.delete(stat.getPath(), false);
						}

					}

					status = fs.listStatus(new Path(PATH+walkers_state_t));
					for (FileStatus stat : status) {

						if (stat.getPath().toString().indexOf("part-r") > -1) {
							fs.rename(stat.getPath(), pt);
						}

					}

					pt = new Path(PATH+walkers_state_t);
					fs.delete(pt,true);	

				}

				pt = new Path(PATH+G_walkers);
				fs.delete(pt,true);

				pr = rt.exec("hadoop jar " + JAR_DIR + "mult.jar mult.MultMatrix " + 
									PATH+walkers_state_t+"_input" + " " + PATH+"tmp" + " " +
									PATH+G_walkers + " B");
		

				pr.waitFor();
				pr.destroy();

				// End of G * walkers_state_t

			
				pt = new Path(PATH+operator_W2B);

				status = fs.listStatus(pt);
				for (FileStatus stat : status) {

					if (stat.getPath().toString().indexOf("part-r") > -1) {
						fs.delete(stat.getPath(), false);
					}

				}		

				status = fs.listStatus(new Path(PATH+G_walkers));
				for (FileStatus stat : status) {

					if (stat.getPath().toString().indexOf("part-r") > -1) {
						fs.rename(stat.getPath(), pt);
					}

				}

				pt = new Path(PATH+W2B_G_walkers);
				fs.delete(pt,true);
			
				pr = rt.exec("hadoop jar " + JAR_DIR + "mult.jar mult.MultMatrix " + 
									PATH+operator_W2B + " " + PATH+"tmp" + " " +
									PATH+W2B_G_walkers + " B");
		

				pr.waitFor();
				pr.destroy();

				// End of W2B * G_walkers

				pt = new Path(PATH+operator_W2A);

				status = fs.listStatus(pt);
				for (FileStatus stat : status) {

					if (stat.getPath().toString().indexOf("part-r") > -1) {
						fs.delete(stat.getPath(), false);
					}

				}

				status = fs.listStatus(new Path(PATH+W2B_G_walkers));
				for (FileStatus stat : status) {

					if (stat.getPath().toString().indexOf("part-r") > -1) {
						fs.rename(stat.getPath(), pt);
					}

				}
			
				pr = rt.exec("hadoop jar " + JAR_DIR + "mult.jar mult.MultMatrix " + 
									PATH+operator_W2A + " " + PATH+"tmp" + " " +
									PATH+walkers_state_t + " B");
		

				pr.waitFor();
				pr.destroy();	

				// End of W2A * W2B_G_walkers
						
				System.out.println("End of the Step "+i);
			}

			if (CLEAN_FOLDERS) {			
				pt = new Path(PATH+operator_W2A);
				fs.delete(pt,true);
				pt = new Path(PATH+operator_W2B);
				fs.delete(pt,true);
				pt = new Path(PATH+walkers_state);
				fs.delete(pt,true);
				pt = new Path(PATH+walkers_state_t+"_input");
				fs.delete(pt,true);
				pt = new Path(PATH+G_walkers);
				fs.delete(pt,true);
				pt = new Path(PATH+W2B_G_walkers);
				fs.delete(pt,true);
			}

			System.out.println("End of the walkers_state_t.");

			// End of the walkers_state
			// --------------------------------------------------------------------------------------------------
			// Start of the walkers_state_norm

			String walkers_state_norm = "walkers_state_norm";

			// Delete the output directory if exists.
			pt = new Path(PATH+walkers_state_norm);
			fs.delete(pt,true);
			
			pr = rt.exec("hadoop jar " + JAR_DIR + "mult.jar mult.NormMatrix " + 
						PATH+walkers_state_t + " " + PATH+walkers_state_norm);			

			pr.waitFor();
			pr.destroy();

			System.out.println("End of the walkers_state_norm.");

			fs.close();			

			// End of the walkers_state_norm
			// --------------------------------------------------------------------------------------------------



			System.out.println("Finished!");
			
			System.out.println("Steps Runtime = "+((System.nanoTime() - startTime)/Math.pow(10,9))+" seconds");

                }catch(Exception e){
                        System.out.println(e);
                }

	}

}


