package mapred.ngramcount;

import java.io.IOException;

import mapred.util.Tokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class NgramCountMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

	//static variable to set n 
	public static int n=1;
	
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		String[] words = Tokenizer.tokenize(line);

		/* Supplied code
		for (String word : words)
			context.write(new Text(word), NullWritable.get());
		*/
		

		//Get number of words in line
		int len = words.length;

		//Emit n-grams
		for(int i=0; i<= len - n; i++){
			String temp = words[i];
			for(int j = i+1; j < (i+n); j++){
				temp += " " + words[j];
			}
			context.write(new Text(temp), NullWritable.get());
		}
	}
}
