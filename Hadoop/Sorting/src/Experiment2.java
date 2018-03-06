import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Experiment2 extends Configured implements Tool {
	private static int reduce_tasks = 10;
	
	@Override
	public int run(String[] args) throws Exception {
		/*** Configure the Hadoop Job **/
		Job job = Job.getInstance(new Configuration());
		
		Configuration conf = job.getConfiguration();
		conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", " ");

		job.setJarByClass(Experiment1.class);
		job.setJobName("Experiment 2 - Custom Partitioner");

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

		/*** Configure the job to use my custom Partitioner **/
		job.setPartitionerClass(StringPartitioner.class);

    	return job.waitForCompletion(true) ? 0 : 1;
	}
	
	/**
	 * Custom partitioner based on partitions from Experiment 1
	 * @author lilannie
	 *
	 */
	public static class StringPartitioner extends Partitioner<Text, Text>{
		public int getPartition(Text key, Text value, int numReduceTasks){
			if (numReduceTasks == 0) return 0; 
			
			String keyText = key.toString();
			
			if (keyText.compareTo("000000000000000") >= 0 &&
					keyText.compareTo("CCCCgggAAAIGGGG") < 0) {
				return 0;
			}
			if (keyText.compareTo("CCCCgggAAAIGGGG") >= 0 &&
					keyText.compareTo("EENNIIIJJMMHIII") < 0) {
				return 1;
			}
			if (keyText.compareTo("EENNIIIJJMMHIII") >= 0 &&
					keyText.compareTo("HHFFaaGGGGIIRRR") < 0) {
				return 2;
			}
			if (keyText.compareTo("HHFFaaGGGGIIRRR") >= 0 &&
					keyText.compareTo("JJJZZZZNNNMMMCW") < 0) {
				return 3;
			}
			if (keyText.compareTo("JJJZZZZNNNMMMCW") >= 0 &&
					keyText.compareTo("MMMMMLLLAAAYYYC") < 0) {
				return 4;
			}
			if (keyText.compareTo("MMMMMLLLAAAYYYC") >= 0 &&
					keyText.compareTo("PPPPjjBgDDRBBJK") < 0) {
				return 5;
			}
			if (keyText.compareTo("PPPPjjBgDDRBBJK") >= 0 &&
					keyText.compareTo("TTLLCCCCEEEEUUA") < 0) {
				return 6;
			}
			if (keyText.compareTo("TTLLCCCCEEEEUUA") >= 0 &&
					keyText.compareTo("XYYYaaaaAAAACCC") < 0) {
				return 7;
			}
			if (keyText.compareTo("XYYYaaaaAAAACCC") >= 0 &&
					keyText.compareTo("eeccAAAAFAAALLL") < 0) {
				return 8;
			}
			if (keyText.compareTo("eeccAAAAFAAALLL") >= 0 &&
					keyText.compareTo("zzzzzzzzzzzzzzz") < 0) {
				return 9;
			}
			
			System.out.println("key did not match: "+keyText);
			return 0;
		
		}
	}
	
	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.out.printf("Two parameters are required - <input> <output>\n");
			System.exit(-1);
		}
		
		/*** Delete any existing directories **/
		Configuration conf = new Configuration();
		Path output = new Path(args[1]);
		FileSystem hdfs = FileSystem.get(conf);

		if (hdfs.exists(output)) hdfs.delete(output, true);
		
		/*** Run the Hadoop Job **/
		int exitCode = ToolRunner.run(new Experiment2(), args);
		
		System.exit(exitCode);
	}
}
