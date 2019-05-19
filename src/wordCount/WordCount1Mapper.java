package wordCount;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class WordCount1Mapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {

	public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
		String $delimiters = "\n";
		StringTokenizer itr = new StringTokenizer(value.toString(), $delimiters);
		while (itr.hasMoreTokens()) {
			String s = itr.nextToken();
			String arr[] = s.split(" ");
			Text t = new Text(arr[0]);
			System.out.println(t);
			output.collect(t, new IntWritable(1));
		}
	}
}