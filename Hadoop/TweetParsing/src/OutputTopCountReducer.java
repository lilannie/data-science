import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.LinkedList;

/**
 * Output Objects with the Top Ten Count
 * @author Annie Steenson
 */
public class OutputTopCountReducer extends Reducer<IntWritable, Text, Text, IntWritable> {
    private LinkedList<String> top10 = new LinkedList<>();
    private Text hashtag = new Text();
    private IntWritable count = new IntWritable();

    public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException  {
        for (Text val : values) {
            top10.add(val.toString()+"\t"+key.toString());

            if (top10.size() > 10) {
                top10.pollFirst();
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        while (!top10.isEmpty()) {
            String[] data = top10.pollLast().split("\t");
            hashtag.set(data[0]);
            count.set(Integer.parseInt(data[1]));
            context.write(hashtag, count);
        }
    }
}