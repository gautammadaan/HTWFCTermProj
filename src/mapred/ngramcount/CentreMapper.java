package mapred.ngramcount;

import java.io.IOException;

import mapred.util.Tokenizer;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

// Get number of clusters important for segregation

public class CentreMapper extends
		Mapper<LongWritable, Text, Text, Text> {


	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		String[] temp = line.split("\t");
		context.write(new Text(temp[0]), new Text(temp[1]));
	}

}