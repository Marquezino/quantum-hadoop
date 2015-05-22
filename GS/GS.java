package gs;

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

public class GS {

	private static final int n = 10;
	private static final String PATH = "/user/david/gs_tmp/";
	private static final String JAR_DIR = "/home/david/Desktop/java/";

	public static void main(String[] args) throws Exception {

		 try{

			long startTime = System.nanoTime();

			long N = (long)Math.pow(2,n);

			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.get(conf);
			FileUtil fu = new FileUtil();

			// Delete the PATH directory if exists. And create a new one empty.
			Path pt = new Path(PATH);
			fs.delete(pt,true);
			fs.mkdirs(pt);

			// ----------------------------------------------------------------------------------------------------
			// Start of psi generation
			String psi = "psi";
			pt = new Path(PATH+"part-r-"+psi);
			BufferedWriter br = new BufferedWriter(new OutputStreamWriter(fs.create(pt,true)));

			for (long i = 0; i < N; i++) {
				br.write(Long.toString(i) + "," + Double.toString(1/Math.sqrt(N)) + "\n");
			}
			br.close();

			System.out.println("Time to generate psi = "+((System.nanoTime() - startTime)/Math.pow(10,9))+" seconds");	

			startTime = System.nanoTime();

			// End of psi generation
			// --------------------------------------------------------------------------------------------------
			// Start of the Steps
			
			int steps = (int)((Math.PI/4)*Math.sqrt(N));

			Runtime rt = Runtime.getRuntime();
			Process pr;

			FileStatus[] status;

			String psi_t = "psi_t";

			// Delete the input directory if exists and create a new one.
			pt = new Path(PATH+psi_t+"_input");
			fs.delete(pt,true);
			fs.mkdirs(pt);

			fu.copy(fs, new Path(PATH+"part-r-"+psi), fs, pt, false, true, conf);

			for (int i = 0; i < steps; i++) {

				if (i > 0) {

					pt = new Path(PATH+psi_t+"_input");
					status = fs.listStatus(pt);
					for (FileStatus stat : status) {

						if (stat.getPath().toString().indexOf("part-r-") > -1) {
							fs.delete(stat.getPath(), false);
						}

					}

					status = fs.listStatus(new Path(PATH+psi_t));
					for (FileStatus stat : status) {

						if (stat.getPath().toString().indexOf("part-r-") > -1) {
							fs.rename(stat.getPath(), pt);
						}

					}

					pt = new Path(PATH+psi_t);
					fs.delete(pt,true);	

				}				

				pr = rt.exec("hadoop jar " + JAR_DIR + "grover.jar grover.Grover " + 
									PATH+psi_t+"_input" + " " + PATH+psi_t + " " +
									Integer.toString(n));

				pr.waitFor();
				pr.destroy();

				System.out.println("End of the Step "+(i+1));
			}




			System.out.println("Finished!");
			
			System.out.println("Steps Runtime = "+((System.nanoTime() - startTime)/Math.pow(10,9))+" seconds");

			// --------------------------------------------------------------------------------------------------
			// End of the Steps

                }catch(Exception e){
                        System.out.println(e);
                }

	}

}

