package mapred.ngramcount;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.*;
import java.io.IOException;
import java.util.*;
import java.io.*;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.*;

public class CentreReducer extends Reducer<Text, Text, Text, Text> {

	int s;
	static int numberOfClusters;
	static int m;
	int[][] pixels = null;

	public void setup(Context context) {
		try {
			Configuration conf = context.getConfiguration();
			m = conf.getInt("weight", 0);
			numberOfClusters = conf.getInt("num",0);

			Path[] files = DistributedCache.getLocalCacheFiles(conf);
			File myFile = new File(files[0].getName());

			BufferedReader cacheReader = new BufferedReader(new FileReader(
					myFile));
			if (files != null && files.length > 0) {
				System.out.println("*****************************"
						+ files[0].getName());

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
					s = pixels.length/(int)Math.sqrt(numberOfClusters);
				}

			}

		} catch (IOException e) {
			System.err.println("Exception reading DistribtuedCache: " + e);
		}
	}

	@Override
	protected void reduce(Text key, Iterable<Text> value, Context context)
			throws IOException, InterruptedException {
		int x = 0;
		int y = 0;
		int sumInt = 0;
		int count = 0;
		StringBuilder sb = new StringBuilder();
		for (Text val : value) {
			String[] temp = val.toString().split(",");
			int xVal = Integer.parseInt(temp[0]);
			int yVal = Integer.parseInt(temp[1]);
			x += xVal;
			y += yVal;
			sb.append(val.toString());
			sb.append(";");
			count++;
			sumInt += pixels[xVal][yVal];
		}
		x = x / count;
		y = y / count;
		int intensity = sumInt / count;
		String fin = x + "," + y + "," + intensity + "|" + s + "|"
				+ sb.toString();
		context.write(key, new Text(fin));
	}

}