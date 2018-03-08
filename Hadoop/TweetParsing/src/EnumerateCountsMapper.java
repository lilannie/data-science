import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Output = ( key = object's count, value = object )
 * @author Annie Steenson
 */
public class EnumerateCountsMapper extends Mapper<Text, Text, IntWritable, Text> {
    public void map(Text key, Text value, Context context) throws IOException, InterruptedException  {
        int count = Integer.parseInt(value.toString());
        context.write(new IntWritable(count), key);
    }
}