import java.io.IOException;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class GlobalClusteringCoefficient {
	static int reduce_tasks = 4;
	static String inputFile = "/cpre419/patents.txt";
//	static String inputFile = "/user/lilannie/lab3_test_exp2.txt";
	static String tempDirectory = "/user/lilannie/lab3/exp2/";

	public static void main(String[] args) throws Exception {
		/** 
		 * Job 1
		 */
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Find neighborhoods");
		job.setJarByClass(GlobalClusteringCoefficient.class);
		job.setNumReduceTasks(reduce_tasks);
		job.setMapperClass(ParseVerticesMapper.class);
		job.setCombinerClass(GetNeighborhoodReducer.class);
		job.setReducerClass(GetNeighborhoodReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(inputFile));
		FileOutputFormat.setOutputPath(job, new Path(tempDirectory+"temp"));
		
		job.waitForCompletion(true);

		/** 
		 * Job 2
		 */
		Configuration conf_two = new Configuration();
		Job job_two = Job.getInstance(conf_two, "Find 2-hop neighborhoods");
		job_two.setJarByClass(GlobalClusteringCoefficient.class);
		job_two.setNumReduceTasks(reduce_tasks);
		job_two.setMapperClass(ParseNeighborsMapper.class);
		job_two.setReducerClass(CalculateTrianglesTripletsReducer.class);

		job_two.setMapOutputKeyClass(Text.class);
		job_two.setMapOutputValueClass(Text.class);

		job_two.setOutputKeyClass(IntWritable.class);
		job_two.setOutputValueClass(IntWritable.class);

		job_two.setInputFormatClass(TextInputFormat.class);
		job_two.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job_two, new Path(tempDirectory+"temp"));
		FileOutputFormat.setOutputPath(job_two, new Path(tempDirectory+"temp2"));
		
		job_two.waitForCompletion(true);
		
		/** 
		 * Job 3
		 */
		Configuration conf_three = new Configuration();
		Job job_three = Job.getInstance(conf_three, "Find GCC");
		job_three.setJarByClass(GlobalClusteringCoefficient.class);
		job_three.setNumReduceTasks(1);
		job_three.setMapperClass(ParseTriangleTripletCounts.class);
		job_three.setReducerClass(CalculateGCCReducer.class);

		job_three.setMapOutputKeyClass(Text.class);
		job_three.setMapOutputValueClass(Text.class);

		job_three.setOutputKeyClass(Text.class);
		job_three.setOutputValueClass(Text.class);

		job_three.setInputFormatClass(TextInputFormat.class);
		job_three.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job_three, new Path(tempDirectory+"temp2"));
		FileOutputFormat.setOutputPath(job_three, new Path(tempDirectory+"output"));
		
		job_three.waitForCompletion(true);
	}
	
	// ASSUME: each edge is undirected
	// Map each vertex to it's neighbor
	public static class ParseVerticesMapper extends Mapper<LongWritable, Text, Text, Text> {
		private Text vertex1 = new Text();
		private Text vertex2 = new Text();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer tokens = new StringTokenizer(value.toString());
			
			vertex1.set(tokens.nextToken());
			vertex2.set(tokens.nextToken());
			context.write(vertex1, vertex2);
			context.write(vertex2, vertex1);
		}
	}

	// Emit the 1-hop neighborhood of each vertex
	public static class GetNeighborhoodReducer extends Reducer<Text, Text, Text, Text> {
		private Text neighborhood = new Text();
		
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			String partial_neighborhood = "";
			for (Text neighbor : values) {
				partial_neighborhood += neighbor + " ";
			}

			neighborhood.set(partial_neighborhood);
			context.write(key, neighborhood);
		}
	}

	public static class TextArrayWritable extends ArrayWritable {
        public TextArrayWritable() {
            super(Text.class);
        }

        public TextArrayWritable(String[] strings) {
            super(Text.class);
            Text[] texts = new Text[strings.length];
            for (int i = 0; i < strings.length; i++) {
                texts[i] = new Text(strings[i]);
            }
            set(texts);
        }
    }

	public static class ParseNeighborsMapper extends Mapper<LongWritable, Text, Text, Text> {
		private StringTokenizer data;
		private String source;

		private StringTokenizer neighbor_data;
		private int num_neighbors;
		private String[] neighbors_array;

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// Get adjacency list
			data = new StringTokenizer(value.toString(), "\t");
			source = data.nextToken();
			
			// Gather neighbors
			neighbor_data = new StringTokenizer(data.nextToken());
			num_neighbors = neighbor_data.countTokens();
			neighbors_array = new String[num_neighbors];
			
			String neighbors = "";
			for (int i = 0; i < num_neighbors; i++) {
				neighbors_array[i] = neighbor_data.nextToken();
				neighbors += neighbors_array[i]+" ";
			}
		
			// For each v in source's neighbors
			// 		emit(v, (source, neighbors))
			for (int i = 0; i < num_neighbors; i++) {
				context.write(new Text(neighbors_array[i]), new Text(source + " "+neighbors));
			}
		}
	}
	
	public static class CalculateTrianglesTripletsReducer extends Reducer<Text, Text, IntWritable, IntWritable> {		
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			int num_triangles = 0;
			int num_triplets = 0;
			LinkedList<String> triplets = new LinkedList<>();
			
			HashSet<String> connectors = new HashSet<>();
			for (Text value: values) {
				System.out.println(value.toString());
				StringTokenizer neighbors = new StringTokenizer(value.toString());

				String connector = neighbors.nextToken();
				connectors.add(connector);
				
				while (neighbors.hasMoreTokens()) {
					String triplet = neighbors.nextToken();
					if (!triplet.equals(key.toString())) {
						triplets.add(triplet);
					}
				}
			}
			
			for (String triplet: triplets) {
				if(connectors.contains(triplet)) {
					num_triangles++;
				}
			}
			
			num_triplets = triplets.size();
			
			context.write(new IntWritable(num_triangles), new IntWritable(num_triplets));
		}
	}

	public static class ParseTriangleTripletCounts extends Mapper<LongWritable, Text, Text, Text> {
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			context.write(new Text("GCC"), value);
		}
	}
	
	public static class CalculateGCCReducer extends Reducer<Text, Text, Text, Text> {
		private StringTokenizer data;
		private int num_triangles;
		private int num_triplets;

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			for (Text value: values) {
				data = new StringTokenizer(value.toString(), "\t");
				// ASSUME input = (<num_triangles>\t<num_triplets>)
				num_triangles += Integer.parseInt(data.nextToken());
				num_triplets += Integer.parseInt(data.nextToken());
			}
			
			num_triangles = num_triangles/6;
			num_triplets = num_triplets/2;

			context.write(key, new Text((3*num_triangles)+"/"+num_triplets));
		}
	}

}
