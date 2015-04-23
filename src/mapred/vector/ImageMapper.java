package mapred.vector;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;

import mapred.util.Tokenizer;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

// Get number of clusters important for segregation

public class ImageMapper extends
		Mapper<LongWritable, Text, Text, Text> {

	static int numberOfClusters; // get this from configuration
	int currentRow = 0;
	public void setup(Context context) {
        Configuration conf = context.getConfiguration();
        numberOfClusters = conf.getInt("num",0);
		System.out.println("*************************Mapper :" + numberOfClusters+ " root:" + (int)Math.sqrt(numberOfClusters));

	}
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		String line = value.toString();
		String lineNumData[] = line.split("\t");
		String data[] = lineNumData[1].split(" ");
		int S = data.length / (int)Math.sqrt(numberOfClusters);
		//int S=9;
		/*
		 * Iterate all the pixel intensity
		 */
		String k = "";
		int yCoordinate = 0;
		for (String pixelIntensity : data) {

			String reducerData = lineNumData[0] + "," + yCoordinate;
			int l = Integer.parseInt(lineNumData[0]);
			k = String.valueOf(l / S) + "#"+String.valueOf(yCoordinate / S);

			// Set cluster center data x,y,i,S:pixels
			if ((((l+1)%S) == 0) && (((yCoordinate + 1)%S) == 0)) 
			{
				reducerData = reducerData + "," + pixelIntensity + "|" + S+"|";
				context.write(new Text(k), new Text(reducerData));					
			}
			else
				context.write(new Text(k), new Text(reducerData));
			yCoordinate++;
		}
	}

}