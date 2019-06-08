package com.company.mapreducetest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WordCount {

	public static class TextMapper extends Mapper<Object, Text, Text, Text> {

		private Text word = new Text();

		@Override
		public void map(Object key, Text value, Context context
				) throws IOException, InterruptedException {
			context.write(value,new Text("") );
		}
	}

	public static class TextReducer
	extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values,
				Context context
				) throws IOException, InterruptedException {

			context.write(key, new Text(""));
		}
	}

	private static final String JOB_NAME = "word count";
	private static final String HDFS_ADDR = "hdfs://cluster1:9000";
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("mapred.job.tracker", "cluster1:9000");
		Job job = Job.getInstance(conf, JOB_NAME);
		job.setJarByClass(WordCount.class);
		job.setMapperClass(TextMapper.class);
		job.setCombinerClass(TextReducer.class);
		job.setReducerClass(TextReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);


		FileInputFormat.addInputPath(job, new Path(HDFS_ADDR+"/test/input"));
		FileOutputFormat.setOutputPath(job, new Path(HDFS_ADDR+"/test/output1"));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
