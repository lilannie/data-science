import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/** 
 * Pass the key and value to be shuffle and sorted and eventually passed to the reducers
 * @author lilannie
 *
 */
public class ParseTextInputMapper extends Mapper<LongWritable, Text, Text, Text> {
	public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
		context.write(key, value);
	}
}
