package cs4650finalproject;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class MapReduceDuplicateCount {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Duplicate Count");
		job.setJarByClass(MapReduceDuplicateCount.class);
		job.setMapperClass(DuplicateCountMapper.class);
		job.setCombinerClass(DuplicateCountReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	
	public static class DuplicateCountMapper extends Mapper<Object, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		// Map function
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String text = value.toString();
			if(text.contains("PostHistoryTypeId=\"10\"") && text.contains("OriginalQuestionIds")) {
				text = text.split("[\\[\\]]")[1];
				word.set(text);
				context.write(word, one);
			}	
		}
	}
	
	public static class DuplicateCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();
		// Reduce function
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			for(IntWritable val: values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}
}
