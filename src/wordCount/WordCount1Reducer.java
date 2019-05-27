package wordCount;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class WordCount1Reducer extends MapReduceBase implements Reducer<Text, DoubleWritable, Text, DoubleWritable> {

	public void reduce(Text t_key, Iterator<DoubleWritable> values, OutputCollector<Text,DoubleWritable> output, Reporter reporter) throws IOException {
		Text key = t_key;
		double sum = 0;
		int count = 0;
		while (values.hasNext()) {
			// replace type of value with the actual type of our value
			DoubleWritable value = (DoubleWritable) values.next();
				sum += value.get();
				count ++;
		}
		output.collect(key, new DoubleWritable(sum/count));
	}
}
