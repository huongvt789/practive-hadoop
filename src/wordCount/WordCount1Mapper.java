package wordCount;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class WordCount1Mapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, DoubleWritable> {

	public void map(LongWritable key, Text value, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {
		String $delimiters = "\n";
		StringTokenizer itr = new StringTokenizer(value.toString(), $delimiters);
		while (itr.hasMoreTokens()) {
			String str = itr.nextToken();
			String list[] = str.split(" ");
			Text deviceId = new Text(list[0]);
			DoubleWritable info = new DoubleWritable(Double.parseDouble(list[1]));
			output.collect(deviceId, info);
		}
	}
}