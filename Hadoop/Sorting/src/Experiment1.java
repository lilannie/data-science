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
		/*** Configure the Hadoop Job **/
		Job job = Job.getInstance(new Configuration());
		
		Configuration conf = job.getConfiguration();
		conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", " ");

		job.setJarByClass(Experiment1.class);
		job.setJobName("Experiment 1 - TotalOrderPartitioner with InputSampler");

		/*** Input and Output paths are specified in the command line arguments **/
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
    
		job.setNumReduceTasks(reduce_tasks);
		/*** Here I use KeyValueTextInputFormat because both the key and the value are type Text **/
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setMapperClass(ParseTextInputMapper.class);
		job.setReducerClass(WriteOutputReducer.class);
	    	
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
	
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		/*** Configure the partitioner **/
		Path partitionFile = new Path(partitionDir);
		TotalOrderPartitioner.setPartitionFile(conf, partitionFile);
		
		/*** Tell the partitioner to use an InputSampler to partition **/
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
	  
		/*** Delete any existing directories **/
		Configuration conf = new Configuration();
		Path output = new Path(args[1]);
		Path partition = new Path(partitionDir);
		FileSystem hdfs = FileSystem.get(conf);

		if (hdfs.exists(output)) hdfs.delete(output, true);
		if (hdfs.exists(partition)) hdfs.delete(partition, true);

		/*** Run the Hadoop Job **/
		int exitCode = ToolRunner.run(new Experiment1(), args);
		
		/*** Delete any temporary files **/
		if (hdfs.exists(partition)) hdfs.delete(partition, true);
		
		System.exit(exitCode);
	}
  
}
