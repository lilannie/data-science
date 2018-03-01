import java.io.IOException;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

public class SignificantPatients {
	static int reduce_tasks = 4;
	static String inputFile = "/cpre419/patents.txt";
	static String tempDirectory = "/user/lilannie/lab3/exp1/";

	public static void main(String[] args) throws Exception {
		/** 
		 * Job 1
		 */
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Find neighborhoods");
		job.setJarByClass(SignificantPatients.class);
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
		job_two.setJarByClass(SignificantPatients.class);
		job_two.setNumReduceTasks(reduce_tasks);
		job_two.setMapperClass(ParseNeighborsMapper.class);
		job_two.setReducerClass(CountSignificanceReducer.class);

		job_two.setMapOutputKeyClass(Text.class);
		job_two.setMapOutputValueClass(Text.class);

		job_two.setOutputKeyClass(Text.class);
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
		Job job_three = Job.getInstance(conf_three, "Find top 10 significant patients");
		job_three.setJarByClass(SignificantPatients.class);
		job_three.setNumReduceTasks(1);
		job_three.setMapperClass(ParseSignificant.class);
		job_three.setReducerClass(FindTopTenSignificant.class);

		job_three.setMapOutputKeyClass(IntWritable.class);
		job_three.setMapOutputValueClass(Text.class);

		job_three.setOutputKeyClass(Text.class);
		job_three.setOutputValueClass(Text.class);

		job_three.setInputFormatClass(TextInputFormat.class);
		job_three.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job_three, new Path(tempDirectory+"temp2"));
		FileOutputFormat.setOutputPath(job_three, new Path(tempDirectory+"output"));
		
		job_three.waitForCompletion(true);
	}

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
	
	public static class CountSignificanceReducer extends Reducer<Text, Text, Text, IntWritable> {		
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			HashSet<String> connected = new HashSet<>();
			for (Text value: values) {
				StringTokenizer neighbors = new StringTokenizer(value.toString());
				while (neighbors.hasMoreTokens()) {
					String vertex = neighbors.nextToken();
					// Count only unique neighbors and do not include self-referred neighbors
					if (!vertex.equals(key.toString()) && !connected.contains(vertex)) {
						connected.add(vertex);
					}
				}
			}
			
			context.write(key, new IntWritable(connected.size()));
		}
	}

	public static class ParseSignificant extends Mapper<LongWritable, Text, IntWritable, Text> {
		private IntWritable count = new IntWritable();
		private Text patient = new Text();
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] data = value.toString().split("\t");
			count.set(Integer.parseInt(data[1]));
			patient.set(data[0]);
			
			// Emit <significance, patient_id)
			context.write(count, patient);
		} 
	} 

	public static class FindTopTenSignificant extends Reducer<IntWritable, Text, Text, Text> {
		private LinkedList<String> top10 = new LinkedList<>();
		private Text count = new Text();
		private Text patient = new Text();
		
		public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			for (Text bigram: values) {
				top10.add(key.toString()+"\t"+bigram.toString());

				if (top10.size() > 10) {
					top10.pollFirst();
				}
			}
		}
		
		@Override
		protected void cleanup(Context context) {
			while (!top10.isEmpty()) {
				String[] data = top10.pollLast().split("\t");
				count.set(data[0]);
				patient.set(data[1]);
				
				try {
					context.write(patient, count);
				} catch (IOException e) {
					e.printStackTrace();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
	} 
}
