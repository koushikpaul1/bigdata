package com.edge.basic;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.PropertyConfigurator;


public class WordCountToolRunner extends Configured implements Tool {
	public static void main(String args[]) throws Exception {
		String log4jConfPath = "log4j.properties";
		PropertyConfigurator.configure(log4jConfPath);
		int res = ToolRunner.run(new WordCountToolRunner(), args);
		System.exit(res);
	}

	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf, "WordCountSampleTemplate");
		job.setJarByClass(WordCountToolRunner.class);
		job.setMapperClass(Map.class);
		job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		Path outputPath = new Path(args[1]);
		FileSystem fs = FileSystem.get(new URI(outputPath.toString()), conf);
		fs.delete(outputPath, true);
		FileOutputFormat.setOutputPath(job, outputPath);
		return job.waitForCompletion(true) ? 0 : 1;
	}

}
