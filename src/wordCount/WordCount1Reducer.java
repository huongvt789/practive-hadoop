package wordCount;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class WordCount1Reducer extends MapReduceBase implements Reducer<Text, DoubleWritable, Text, DoubleWritable> {

	public void reduce(Text t_key, Iterator<DoubleWritable> values, OutputCollector<Text,DoubleWritable> output, Reporter reporter) throws IOException {
		Text key = t_key;
		double minInfo = Double.POSITIVE_INFINITY; //else: Double.Nagative_I...... (tim max)
		System.out.println(values);
		while (values.hasNext()) {
			// replace type of value with the actual type of our value
			DoubleWritable value = (DoubleWritable) values.next();
			double info = value.get();
			minInfo = minInfo < info ? minInfo : info;
		}
		output.collect(key, new DoubleWritable(minInfo));
	}
}
