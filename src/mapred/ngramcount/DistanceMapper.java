package mapred.ngramcount;

import java.io.IOException;
import java.util.*;
import java.io.*;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.*;

import mapred.util.Tokenizer;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

// Get number of clusters important for segregation

public class DistanceMapper extends Mapper<LongWritable, Text, Text, Text> {

	static int m;
	int[][] pixels = null;

	@Override
	public void setup(Context context) {
		try {
			Configuration conf = context.getConfiguration();
			m = conf.getInt("weight", 0);

			Path[] files = DistributedCache.getLocalCacheFiles(conf);
			File myFile = new File(files[0].getName());

			BufferedReader cacheReader = new BufferedReader(new FileReader(
					myFile));
			if (files != null && files.length > 0) {

				String line;
				int i = 0;
				try {
					while ((line = cacheReader.readLine()) != null) {
						String[] temp = line.split("\t");
						String[] p = temp[1].split(" ");
						if (pixels == null) {
							pixels = new int[p.length][p.length];
						}
						for (int j = 0; j < p.length; j++) {
							pixels[i][j] = Integer.parseInt(p[j]);
						}
						i++;
					}
				} finally {
					cacheReader.close();
				}

			}

		} catch (IOException e) {
			System.err.println("Exception reading DistribtuedCache: " + e);
		}
	}

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String line = value.toString();

		// String[] words = Tokenizer.tokenize(line);
		String[] temp = line.split("\t");
		String[] vector = temp[1].split("\\|");
		String[] centre = vector[0].split(",");
		int x = Integer.parseInt(centre[0]);
		int y = Integer.parseInt(centre[1]);
		int c_int = Integer.parseInt(centre[2]);
		int s = Integer.parseInt(vector[1]);

		int rmin = Math.max(x - s, 0);
		int rmax = Math.min(x + s, pixels.length);
		int cmin = Math.max(y - s, 0);
		int cmax = Math.min(y + s, pixels[0].length);

		for (int i = rmin; i < (rmax); i++) {
			for (int j = cmin; j < (cmax); j++) {
				// for (int i=0;i<pixels.length;i++){
				// for (int j=0;j<pixels[0].length;j++){
				int p_int = pixels[i][j];
				double dc = (double) Math.sqrt((double) (Math.pow(
						(double) (p_int - c_int), 2)));
				double ds = (double) Math.sqrt((double) (Math.pow(
						(double) (i - x), 2) + Math.pow((double) (j - y), 2)));
				double dis = (double) Math.sqrt((double) (Math.pow((double) dc,
						2) + Math.pow((double) (ds / s), 2) * m * m));
				String k = i + "," + j;
				String v = temp[0] + ":" + dis;
				context.write(new Text(k), new Text(v));
			}
		}

	}

}
