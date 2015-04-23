package mapred.ngramcount;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.*;

public class DistanceReducer extends Reducer<Text, Text, Text, Text> {

	@Override
	protected void reduce(Text key, Iterable<Text> value, Context context)
			throws IOException, InterruptedException {
		String label ="";
		double min = Double.MAX_VALUE;
		for (Text val : value) {
			String[] temp = val.toString().split(":");
			if(Double.parseDouble(temp[1])< min){
				label = temp[0];
			}
		}
		context.write(new Text(label), key);
	}

}