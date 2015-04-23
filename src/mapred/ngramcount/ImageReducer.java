package mapred.ngramcount;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.*;

public class ImageReducer extends Reducer<Text, Text, Text, Text> {

@Override
protected void reduce(Text key, Iterable<Text> value, Context context)
throws IOException, InterruptedException 
{

	String cen ="";

	StringBuilder sb = new StringBuilder();

	for(Text val: value )
	{

		String s = val.toString();

		if(s.endsWith("|"))
			cen = s;
		else
		{
			sb.append(s);
			sb.append(";");
		}

	}

	Text v = new Text(cen+sb.toString()); 

	context.write(key, v);

	}

}