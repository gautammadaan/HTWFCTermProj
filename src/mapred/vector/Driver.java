package mapred.vector;

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
		int n = parser.getInt("n");
		getImageFeatureVector(input, output, n);

	}

	private static void getImageFeatureVector(String input, String output,
			int clus) throws IOException, ClassNotFoundException,
			InterruptedException {
		Configuration conf = new Configuration();
		Optimizedjob job;
		conf.setInt("num", clus);
		job = new Optimizedjob(conf, input, output, "Compute vectors");
		job.setClasses(ImageMapper.class, ImageReducer.class, null);
		job.setMapOutputClasses(Text.class, Text.class);

		job.run();
		job.waitForCompletion(true);

	}
}
