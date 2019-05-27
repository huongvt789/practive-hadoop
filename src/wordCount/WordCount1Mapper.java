package wordCount;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class WordCount1Mapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, DoubleWritable> {

	private Text product = new Text();
	private DoubleWritable price = new DoubleWritable();
	public void map(LongWritable key, Text value, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {
			
			String[] array = value.toString().split(",");
			String productName = array[1];
			double priceNum = Double.parseDouble(array[2]);
			System.out.println(array);
			product.set(productName);
			price.set(priceNum);
			
			
			output.collect(product, price);
		}
}