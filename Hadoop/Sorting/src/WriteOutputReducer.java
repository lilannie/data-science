import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Here are assume that keys can have multiple values.
 * We also assume that the order the reducer receives the keys is in sorted order
 * Output the key and value.
 * @author lilannie
 *
 */
public class WriteOutputReducer extends Reducer<Text, Text, Text, Text> {
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		for (Text value : values) {
			context.write(key, value);
		}
	}
}