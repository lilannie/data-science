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

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;

public class TopProflificHashtag {
	private static String tempFile = "/user/lilannie/lab5/exp3/temp";

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();

		/*** Delete any existing directories **/
		FileSystem hdfs = FileSystem.get(conf);
		Path output = new Path(args[1]);
		if (hdfs.exists(output)) hdfs.delete(output, true);
		Path temp = new Path(tempFile);
		if (hdfs.exists(temp)) hdfs.delete(temp, true);

		/*** Create First Job ***/
		Job countHashtags = createJob(conf, "Experiment 3 - Count Tweets Per User", 4, args[0], tempFile);

		/*** Configure mapper and reducer ***/
		countHashtags.setMapperClass(GroupTweets.class);
		countHashtags.setReducerClass(OutputUserTweets.class);
		countHashtags.setMapOutputKeyClass(Text.class);
		countHashtags.setMapOutputValueClass(Text.class);
		countHashtags.setOutputKeyClass(Text.class);
		countHashtags.setOutputValueClass(Text.class);

		/*** Configure input and output ***/
		countHashtags.setInputFormatClass(JsonInputFormat.class);
		countHashtags.setOutputFormatClass(TextOutputFormat.class);

		/*** Run First Job ***/
		countHashtags.waitForCompletion(true);

		/*** Create Second Job ***/
		conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", "\t");
		Job topTenHashtags = createJob(conf, "Experiment 3 - Top Ten Prolific Hashtag", 1, tempFile, args[1]);

		/*** Configure mapper and reducer ***/
		topTenHashtags.setMapperClass(CountTweets.class);
		topTenHashtags.setReducerClass(OutputProlificHashtags.class);
		topTenHashtags.setMapOutputKeyClass(IntWritable.class);
		topTenHashtags.setMapOutputValueClass(Text.class);
		topTenHashtags.setOutputKeyClass(Text.class);
		topTenHashtags.setOutputValueClass(Text.class);

		/*** Configure input and output ***/
		topTenHashtags.setInputFormatClass(KeyValueTextInputFormat.class);
		topTenHashtags.setOutputFormatClass(TextOutputFormat.class);

		/*** Run Second Job ***/
		topTenHashtags.waitForCompletion(true);

		/*** Clean up temp directory ***/
		if (hdfs.exists(temp)) hdfs.delete(temp, true);
	}

	/******* First Job Mapper *******/
	public static class GroupTweets extends Mapper<LongWritable, Text, Text, Text> {
		private JSONParser parser = new JSONParser();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException  {
			try {
				JSONObject tweet = (JSONObject) parser.parse(value.toString());
				JSONObject user = (JSONObject) tweet.get("user");

				String screen_name = user.get("screen_name").toString();

				context.write(new Text(screen_name), value);
			} catch (ParseException e) {
				e.printStackTrace();
			}
		}
	}

	/******* First Job Reducer *******/
	public static class OutputUserTweets extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException  {
			String tweets = "[";
			for (Text tweet : values) {
				tweets += tweet.toString();
			}
			context.write(key, new Text(tweets));
		}
	}

	/******* First Job Mapper *******/
	public static class CountTweets extends Mapper<Text, Text, IntWritable, Text> {
		private JSONParser parser = new JSONParser();

		public void map(Text key, Text value, Context context) throws IOException, InterruptedException  {
			try {
				JSONArray userTweets = (JSONArray) parser.parse(value.toString());

				context.write(new IntWritable(userTweets.size()), value);
			} catch (ParseException e) {
				e.printStackTrace();
			}
		}
	}

	/******* First Job Reducer *******/
	public static class OutputProlificHashtags extends Reducer<IntWritable, Text, Text, Text> {
		private JSONParser parser = new JSONParser();
		private LinkedList<String> top10 = new LinkedList<>();
		private Text userHashtag = new Text();
		private IntWritable count = new IntWritable();

		public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException  {
			for (Text userTweets : values) {
				top10.add(userTweets.toString());

				if (top10.size() > 10) {
					top10.pollFirst();
				}
			}
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			while (!top10.isEmpty()) {
				try {
					String userTweets = top10.pollLast();
					String screen_name = null;
					JSONArray tweets = (JSONArray) parser.parse(userTweets);

					HashMap<String, Integer> hashtagCount = new HashMap<>();

					int max = Integer.MIN_VALUE;
					String mostCommonHashtag = "";

					for (Object tweetObj: tweets) {
						JSONObject tweet = (JSONObject) tweetObj;

						if (screen_name == null) {
							JSONObject user = (JSONObject) tweet.get("user");
							screen_name = user.get("screen_name").toString();
						}

						JSONObject entities = (JSONObject) tweet.get("entities");
						JSONArray hashtags = (JSONArray) entities.get("hashtags");
						for (Object tag: hashtags) {
							JSONObject tagInfo = (JSONObject) tag;
							String hashtag = tagInfo.get("text").toString();

							int count = hashtagCount.getOrDefault(hashtag, 0) + 1;
							hashtagCount.put(hashtag, count);

							if (max < count) {
								max = count;
								mostCommonHashtag = hashtag;
							}
						}

					}

					context.write(new Text("Screen name: "+screen_name), new Text(" Most Common Hashtag: "+mostCommonHashtag));
				} catch (ParseException e) {
					e.printStackTrace();
				}


			}
		}
	}

	/******* HELPERS *******/
	private static Job createJob(Configuration conf, String name, int reduce_tasks, String inputFile, String outputFile) throws IOException {
		// Create a Hadoop Job
		Job job = Job.getInstance(conf, name);

		// Attach the job to this Class
		job.setJarByClass(TopProflificHashtag.class);

		// Number of reducers
		job.setNumReduceTasks(reduce_tasks);

		// Input path
		FileInputFormat.addInputPath(job, new Path(inputFile));

		// Output path
		FileOutputFormat.setOutputPath(job, new Path(outputFile));

		return job;
	}
}
