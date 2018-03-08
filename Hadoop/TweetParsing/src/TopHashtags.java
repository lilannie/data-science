import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.util.HashSet;

/**
 * @author Annie Steenson
 */
public class TopHashtags {
    private static String tempFile = "/user/lilannie/lab5/exp1/temp";

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();

        /*** Delete any existing directories **/
        FileSystem hdfs = FileSystem.get(conf);
        Path output = new Path(args[1]);
        if (hdfs.exists(output)) hdfs.delete(output, true);
        Path temp = new Path(tempFile);
        if (hdfs.exists(temp)) hdfs.delete(temp, true);

        /*** Create First Job ***/
        Job countHashtags = createJob(conf, "Experiment 1 - Count Hashtags", 4, args[0], tempFile);

        /*** Configure mapper and reducer ***/
        countHashtags.setMapperClass(EnumerateHashtags.class);
        countHashtags.setReducerClass(CountHashtags.class);
        countHashtags.setMapOutputKeyClass(Text.class);
        countHashtags.setMapOutputValueClass(IntWritable.class);
        countHashtags.setOutputKeyClass(Text.class);
        countHashtags.setOutputValueClass(IntWritable.class);

        /*** Configure input and output ***/
        countHashtags.setInputFormatClass(JsonInputFormat.class);
        countHashtags.setOutputFormatClass(TextOutputFormat.class);

        /*** Run First Job ***/
        countHashtags.waitForCompletion(true);

        /*** Create Second Job ***/
        conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", "\t");
        Job topTenHashtags = createJob(conf, "Experiment 1 - Top Ten Hashtags", 1, tempFile, args[1]);

        /*** Configure mapper and reducer ***/
        topTenHashtags.setMapperClass(EnumerateCountsMapper.class);
        topTenHashtags.setReducerClass(OutputTopCountReducer.class);
        topTenHashtags.setMapOutputKeyClass(IntWritable.class);
        topTenHashtags.setMapOutputValueClass(Text.class);
        topTenHashtags.setOutputKeyClass(Text.class);
        topTenHashtags.setOutputValueClass(IntWritable.class);

        /*** Configure input and output ***/
        topTenHashtags.setInputFormatClass(KeyValueTextInputFormat.class);
        topTenHashtags.setOutputFormatClass(TextOutputFormat.class);

        /*** Run Second Job ***/
        topTenHashtags.waitForCompletion(true);

        /*** Clean up temp directory ***/
        if (hdfs.exists(temp)) hdfs.delete(temp, true);
	}

    /******* First Job Map
     *
     * For every unique hashtag:
     *      emit (key = 1, value = hashtag)
     *
     *******/
    public static class EnumerateHashtags extends Mapper<LongWritable, Text, Text, IntWritable>  {
        private JSONParser parser = new JSONParser();
        private IntWritable one = new IntWritable(1);
        public static final Log log = LogFactory.getLog(EnumerateHashtags.class);

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException  {
            try {
                JSONObject tweet = (JSONObject) parser.parse(value.toString());
                JSONObject entities = (JSONObject) tweet.get("entities");

                emitHashtags(entities, context);
            } catch (ParseException e) {
                log.error(e.toString());
            }
		}

		private void emitHashtags(JSONObject entities, Context context) throws IOException, InterruptedException {
            JSONArray hashtags = (JSONArray) entities.get("hashtags");
            HashSet<String> unique_tags = new HashSet<>();

            for (Object tag: hashtags) {
                JSONObject tagInfo = (JSONObject) tag;
                unique_tags.add(tagInfo.get("text").toString().toLowerCase());
            }

            for (String tag: unique_tags) {
                context.write(new Text(tag), one);
            }
        }
	}

    /******* First Job Reduce
     *
     * For every unique hashtag:
     *      emit (key = hashtag, value = count )
     *
     *******/
	public static class CountHashtags extends Reducer<Text, IntWritable, Text, IntWritable>  {
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException  {
		    int count = 0;
			for (IntWritable val : values) {
				count++;
			}
			context.write(key, new IntWritable(count));
		}
	}

    /******* HELPERS
     *
     * Creates a Job with the setup calls
     *
     *******/
    private static Job createJob(Configuration conf, String name, int reduce_tasks, String inputFile, String outputFile) throws IOException {
        // Create a Hadoop Job
        Job job = Job.getInstance(conf, name);

        // Attach the job to this Class
        job.setJarByClass(TopHashtags.class);

        // Number of reducers
        job.setNumReduceTasks(reduce_tasks);

        // Input path
        FileInputFormat.addInputPath(job, new Path(inputFile));

        // Output path
        FileOutputFormat.setOutputPath(job, new Path(outputFile));

        return job;
    }
}
