package com.company.mapreducetest;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.BasicConfigurator;

public class Sort {

	public static class SortMapper extends Mapper<Object, Text, IntWritable, NullWritable> {

		private IntWritable outKey = new IntWritable();
		private NullWritable outValue = NullWritable.get();

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			if (line != null && !line.equals("")) {

				outKey.set(Integer.parseInt(line));
				context.write(outKey, outValue);
			}
		}
	}

	public static class SortReducer	extends Reducer<IntWritable, NullWritable, IntWritable, IntWritable> {
		private IntWritable outKey = new IntWritable(1);

		@Override
		public void reduce(IntWritable key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {

			for (NullWritable ignored : values) {
				context.write(outKey, key);
				outKey.set(outKey.get() + 1);
			}
		}
	}


	private static final String JOB_NAME = "sort";
	private static final String HDFS_ADDR = "hdfs://cluster1:9000";
	private static final String IN_PATH = HDFS_ADDR + "/sort/input";
	private static final String OUT_PATH = HDFS_ADDR + "/sort/output";

	public static void main(String[] args) throws Exception {
		BasicConfigurator.configure();
//		System.setProperty("hadoop.home.dir", "/usr/local/hadoop-2.9.2");
		Configuration conf = new Configuration();
		FileSystem fileSystem = FileSystem.get(new URI(OUT_PATH), conf);
		if (fileSystem.exists(new Path(OUT_PATH))) {
			fileSystem.delete(new Path(OUT_PATH), true);
		}

		conf.set("mapred.job.tracker", "cluster1:9000");
		Job job = Job.getInstance(conf, JOB_NAME);
		job.setJarByClass(Sort.class);
		job.setReducerClass(SortReducer.class);

		job.setInputFormatClass(TextInputFormat.class);
		FileInputFormat.addInputPath(job, new Path(IN_PATH));
		job.setMapperClass(SortMapper.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(NullWritable.class);


		job.setOutputFormatClass(TextOutputFormat.class);
		FileOutputFormat.setOutputPath(job, new Path(OUT_PATH));
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
