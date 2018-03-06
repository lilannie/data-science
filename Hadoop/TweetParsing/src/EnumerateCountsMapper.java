import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class EnumerateCountsMapper extends Mapper<Text, Text, IntWritable, Text> {
    public void map(Text key, Text value, Context context) throws IOException, InterruptedException  {
        int hashtagCount = Integer.parseInt(value.toString());
        context.write(new IntWritable(hashtagCount), key);
    }
}