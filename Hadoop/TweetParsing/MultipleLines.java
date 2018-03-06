import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.LineReader;

/* CPRE 419x Sample custom input format */

public class MultipleLines {
	
    public static void main(String[] args) throws Exception {
		
	    Configuration conf = new Configuration();
	    
	    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		
	    int reduce_tasks = 4;
		
        // Create a Hadoop Job
	    Job job = Job.getInstance(conf, "CustomInputFormat using MapReduce");
        
        // Attach the job to this Class
	    job.setJarByClass(MultipleLines.class);
        
        // Number of reducers
        job.setNumReduceTasks(reduce_tasks);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        // Set the Map class
	    job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        
        // Set how the input is split
        // TextInputFormat.class splits the data per line
	    job.setInputFormatClass(MultipleLineInputFormat.class);
        
        // Output format class
	    job.setOutputFormatClass(TextOutputFormat.class);
        
        // Input path
	    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        
        // Output path
	    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        
        // Run the job
	    job.waitForCompletion(true);
    } 
	
    // The Map Class
    public static class Map extends Mapper<LongWritable, Text, Text, Text>  {
        // The map method 
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException  {

                context.write(new Text(key.toString()), new Text(value));
        }
    } 
	
    // The Reduce class
    public static class Reduce extends Reducer<Text, Text, Text, Text>  {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException  {
            for (Text val : values) {

                context.write(key, val);
            }
        }
    }

    // Create a Custom Input Format
    // Subclass from FileInputFormat class that provides 
    // much of the basic handling necessary to manipulate files.
    public static class MultipleLineInputFormat extends FileInputFormat <LongWritable, Text> {
        @Override 
        public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
            return new CustomRecordReader();
        }
    }
    
    // Create a custom Record Locator
    public static class CustomRecordReader extends RecordReader <LongWritable, Text> {
                                                        
        private LineReader lineReader;
        private LongWritable key;
        private Text value;
        long start, end, position, no_of_calls;
                                                        
        @Override
        public void initialize(InputSplit genericSplit, TaskAttemptContext context) throws IOException {
            
            Configuration conf = context.getConfiguration();
            
            FileSplit split = (FileSplit) genericSplit;
            final Path file = split.getPath();
            FileSystem fs = file.getFileSystem(conf);
            
            start = split.getStart();
            end = start + split.getLength();
            position = start;
            
            FSDataInputStream input_file = fs.open(split.getPath());
            input_file.seek(start);
                                                                
            lineReader = new LineReader(input_file, conf); 
            
            no_of_calls = 0;
        }  
             
        @Override
        public float getProgress() throws IOException, InterruptedException {
            if (start == end) {
                return 0.0f;
            }
            else {   
                return Math.min(1.0f, (position - start) / (float)(end - start));
            }
        }
                                                        
        @Override
        public LongWritable getCurrentKey() throws IOException, InterruptedException {                                                         
            return key;
        }
                                                        
        @Override
        public Text getCurrentValue() throws IOException, InterruptedException {                                                   
            return value;
        }
                                                        
        @Override                                                
        public boolean nextKeyValue() throws IOException {
            no_of_calls = no_of_calls + 1;
            
            if (position == end)  {          
                return false;
            }
                                                            
            if (key == null) {          
                key = new LongWritable();
            }
            
            if (value == null) {    
                value = new Text(" ");
            }
            
            key.set(no_of_calls);
            
            Text temp_text = new Text(" ");
            
            int read_length = lineReader.readLine(temp_text);
            
            String temp = temp_text.toString();
            
            position = position + read_length;
            
            read_length = lineReader.readLine(temp_text);
            
            temp = temp + temp_text.toString();
            
            position = position + read_length;
            
            value.set(temp);
                                             
            return true;
        }
              
        @Override
        public void close() throws IOException {

            if ( lineReader != null )
                lineReader.close();
        }
        
    }
}