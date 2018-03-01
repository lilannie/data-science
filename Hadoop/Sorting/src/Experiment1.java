import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Experiment1 extends Configured implements Tool {
	private static int reduce_tasks = 10;
	private static String partitionDir = "/user/lilannie/lab4/exp1/partition";
	
	@Override
	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(new Configuration());
		
		Configuration conf = job.getConfiguration();
		conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", " ");

		job.setJarByClass(Experiment1.class);
		job.setJobName("Experiment 1 - TotalOrderPartitioner with InputSampler");

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
    
		job.setNumReduceTasks(reduce_tasks);
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setMapperClass(ParseTextInputMapper.class);
		job.setReducerClass(WriteOutputReducer.class);
	    	
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
	
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		Path partitionFile = new Path(partitionDir);
		TotalOrderPartitioner.setPartitionFile(conf, partitionFile);
		
		InputSampler.Sampler<Text, Text> sampler = new InputSampler.RandomSampler<Text, Text>(0.1, 10000, 10);
		InputSampler.writePartitionFile(job, sampler);

		job.setPartitionerClass(TotalOrderPartitioner.class);

    	return job.waitForCompletion(true) ? 0 : 1;
	}
  
	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.out.printf("Two parameters are required - <input> <output>\n");
			System.exit(-1);
		}
	  
		Configuration conf = new Configuration();
		Path output = new Path(args[1]);
		Path partition = new Path(partitionDir);
		FileSystem hdfs = FileSystem.get(conf);

		// delete existing directory
		if (hdfs.exists(output)) hdfs.delete(output, true);
		if (hdfs.exists(partition)) hdfs.delete(partition, true);

		int exitCode = ToolRunner.run(new Experiment1(), args);
		
		// delete existing directory
		if (hdfs.exists(partition)) hdfs.delete(partition, true);
		
		System.exit(exitCode);
	}
  
}
