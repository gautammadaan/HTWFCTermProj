package mapred.ngramcount;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.io.IOException;
import mapred.job.Optimizedjob;
import mapred.util.SimpleParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import mapred.*;

public class Driver {

	public static void main(String args[]) throws Exception {
		SimpleParser parser = new SimpleParser(args);

		String input = parser.get("input");
		String output = parser.get("output");
		String tmp = parser.get("tmp");
		int m = parser.getInt("m");
		int i = parser.getInt("i");
		int n = parser.getInt("n"); // Define this in	// parser as well
		CentreReducer.numberOfClusters = parser.getInt("n");
		getImageFeatureVector(input, output, tmp, i, n, m);

	}

	private static void getImageFeatureVector(String input, String output,
			String tmp, int i, int clus, int weight) throws IOException, ClassNotFoundException,
			InterruptedException {
		System.out.println(input + output + tmp);
		Configuration conf = new Configuration();
	//	FileSystem fs = FileSystem.get(conf);
	//	Path cachefile = new Path(input);
//		FileStatus[] list = fs.globStatus(cachefile);
		Path hdfsPath = new Path(input);
		DistributedCache.addCacheFile(hdfsPath.toUri(), conf);
	/*	for (FileStatus status : list) {
			DistributedCache.addCacheFile(status.getPath().toUri(), conf);
		}
		*/
		Optimizedjob job;
		int iter = 1;
		conf.setInt("num",clus);
		conf.setInt("weight", weight);
		if (i == 1) {
			job = new Optimizedjob(conf, input, output, "Compute NGram Count");
		} else {
			job = new Optimizedjob(conf, input, tmp+iter, "Compute NGram Count");
		}
		job.setClasses(ImageMapper.class, ImageReducer.class, null);
		job.setMapOutputClasses(Text.class, Text.class);
		
		job.run();
		job.waitForCompletion(true);
		
		while(iter < i){
			iter++;
			if (iter == i) {
				job = new Optimizedjob(conf, tmp+(iter-1), output+"inter", "Compute NGram Count");
			} else {
				job = new Optimizedjob(conf, tmp+(iter-1), tmp+"inter"+iter, "Compute NGram Count");
			}
			job.setClasses(DistanceMapper.class, DistanceReducer.class, null);
			job.setMapOutputClasses(Text.class, Text.class);

			job.run();
			job.waitForCompletion(true);
			
			if (iter == i) {
				job = new Optimizedjob(conf, output+"inter", output, "Compute NGram Count");
			} else {
				job = new Optimizedjob(conf, tmp+"inter"+iter, tmp+iter, "Compute NGram Count");
			}
			job.setClasses(CentreMapper.class, CentreReducer.class, null);
			job.setMapOutputClasses(Text.class, Text.class);

			job.run();
			job.waitForCompletion(true);
			
		}
	}
}
