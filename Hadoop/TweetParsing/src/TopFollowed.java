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
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.IOException;

/**
 * @author Annie Steenson
 */
public class TopFollowed {
	private static String tempFile = "/user/lilannie/lab5/exp2/temp";

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();

		/*** Delete any existing directories **/
		FileSystem hdfs = FileSystem.get(conf);
		Path output = new Path(args[1]);
		if (hdfs.exists(output)) hdfs.delete(output, true);
		Path temp = new Path(tempFile);
		if (hdfs.exists(temp)) hdfs.delete(temp, true);

		/*** Create First Job ***/
		Job countHashtags = createJob(conf, "Experiment 2 - Count Followers", 4, args[0], tempFile);

		/*** Configure mapper and reducer ***/
		countHashtags.setMapperClass(EnumerateFollowers.class);
		countHashtags.setReducerClass(CountFollowers.class);
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
		Job topTenHashtags = createJob(conf, "Experiment 2 - Top Ten Followed", 1, tempFile, args[1]);

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

	/******* First Job Mapper
	 *
	 *	For every tweet:
	 *		emit( key = user, value = user's follower count )
	 *
	 *******/
	public static class EnumerateFollowers extends Mapper<LongWritable, Text, Text, IntWritable> {
		private JSONParser parser = new JSONParser();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException  {
			try {
				JSONObject tweet = (JSONObject) parser.parse(value.toString());
				JSONObject user = (JSONObject) tweet.get("user");

				String screen_name = user.get("screen_name").toString();
				int followers_count = Integer.parseInt(user.get("followers_count").toString());

				context.write(new Text(screen_name), new IntWritable(followers_count));
			} catch (ParseException e) {
				e.printStackTrace();
			}
		}
	}

	/******* First Job Reducer
	 *
	 *	For every user:
	 *		emit( key = user, value = total follower count )
	 *
	 *******/
	public static class CountFollowers extends Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException  {
			int max = Integer.MIN_VALUE;
			for (IntWritable val : values) {
				if (max < val.get())
					max = val.get();
			}
			context.write(key, new IntWritable(max));
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
		job.setJarByClass(TopFollowed.class);

		// Number of reducers
		job.setNumReduceTasks(reduce_tasks);

		// Input path
		FileInputFormat.addInputPath(job, new Path(inputFile));

		// Output path
		FileOutputFormat.setOutputPath(job, new Path(outputFile));

		return job;
	}
}
