package com.edge.basic;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * What happens when you run MapReduce without setting a mapper or a
 * reducer?Each line is an integer followed by a tab character, followed by the
 * original weather data record. MinimalMapReduceWithDefaults shows a program
 * that has exactly the same effect as MinimalMapReduce, but explicitly sets the
 * job settings to their defaults.The default input format is TextInputFormat,
 * which produces keys of type LongWritable (the offset of the beginning of the
 * line in the file) and values of type Text (the line of text). This explains
 * where the integers in the final output come from: they are the line offsets.
 */

/** data/misc/NCDC output/MinimalMapReduceWithDefaults */

public class MinimalMapReduceWithDefaults extends Configured implements Tool {
	public int run(String[] args) throws Exception {
		Configuration conf = this.getConf();
		Job job = Job.getInstance(conf, "MinimalMapReduceWithDefaults");
		job.setJarByClass(getClass());
		job.setInputFormatClass(TextInputFormat.class);
		job.setMapperClass(Mapper.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setPartitionerClass(HashPartitioner.class);
		job.setNumReduceTasks(1);
		job.setReducerClass(Reducer.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new MinimalMapReduceWithDefaults(), args);
		System.exit(exitCode);
	}
}