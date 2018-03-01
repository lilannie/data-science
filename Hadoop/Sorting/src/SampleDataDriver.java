import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;

public class SampleDataDriver extends Configured implements Tool {
	private static int reduce_tasks = 10;

	@Override
	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(new Configuration());
		
		Configuration conf = job.getConfiguration();
		conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", " ");

		job.setJarByClass(Experiment1.class);
		job.setJobName("Exp 2 - Sample Data Driver");

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
    
		job.setNumReduceTasks(reduce_tasks);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setMapperClass(RandomSampleMapper.class);
		job.setReducerClass(RandomSampleReducer.class);
	    	
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
	
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

    	return job.waitForCompletion(true) ? 0 : 1;
	}
	
	public static class RandomSampleMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			context.write(new IntWritable(randomWithRange(0, 10)), value);
		}
		
		private int randomWithRange(int min, int max){
		   int range = (max - min) + 1;     
		   return (int)(Math.random() * range) + min;
		}
	}

	public static class RandomSampleReducer extends Reducer<IntWritable, Text, Text, Text> {
		public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			for (Text value : values) {
				// Assumed data is split by spaces
				String[] data = value.toString().split(" ");
				context.write(new Text(data[0]), new Text(data[1]));
			}			
		}
	}
}

 
