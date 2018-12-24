package com.edge.basic;

// == MinimalMapReduce The simplest possible MapReduce driver, which uses the defaults
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
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

/** data/misc/NCDC output/MinimalMapReduce */

public class MinimalMapReduce extends Configured implements Tool {

	public int run(String[] args) throws Exception {
		Configuration conf = this.getConf();
		Job job = Job.getInstance(conf, "MinimalMapReduce");
		job.setJarByClass(getClass());
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new MinimalMapReduce(), args);
		System.exit(exitCode);
	}
}