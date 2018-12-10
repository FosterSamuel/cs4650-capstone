package cs4650finalproject;

/**
 * CS4650 Final Project: StackExchange Duplicate Question Analyzer
 * Authors: Carlos Hernandez, Samuel Foster, Joseph Tuazon
 * Date: December 2018
 * 
 * Taking in the PostHistory.XML of a StackExchange community, posts that are
 * closed as duplicates are extracted and the original questions they are 
 * linked to are counted.
 * 
 */

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

public class MapReduceDuplicateCount {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Duplicate Count");
		
		job.setJarByClass(MapReduceDuplicateCount.class);
		job.setMapperClass(DuplicateCountMapper.class);
		job.setReducerClass(DuplicateCountReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

	/**
	 * Each entry inside the PostHistory file has a number of attributes. One of
	 * them, PostHistoryId, gives away the reason for a type of post. Type '10'
	 * signifies the post was closed as a duplicate. We then to check to see if
	 * there are Ids attached pointing us to the original posts. If there are,
	 * put them as individual keys.
	 */
	public static class DuplicateCountMapper extends Mapper<Object, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Logger log = Logger.getLogger(DuplicateCountMapper.class);
		
		private Text word = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			log.info("Value: " + value);
			String text = value.toString();
			if (text.contains("PostHistoryTypeId=\"10\"") && text.contains("OriginalQuestionIds")) {
				text = text.split("[\\[\\]]")[1];
				
				log.info("Text after first split: " + text);
				
				for(String linkId : text.split(",")) {
					word.set("ID" + linkId.replaceAll("\\s", ""));
					log.info("Word: " + word);
					context.write(word, one);
				}
			}
			log.info("=================");
		}
	}

	/**
	 * With the original Ids in place, we reduce them to count
	 * them. Output to file.
	 */
	public static class DuplicateCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			
			result.set(sum);
			context.write(key, result);
		}
	}
}
