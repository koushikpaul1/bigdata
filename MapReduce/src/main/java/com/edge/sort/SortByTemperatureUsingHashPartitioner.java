package com.edge.sort;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.PropertyConfigurator;


/**
 * This program's input is a sequencefile with intwritable keys (run
 * SortDataPreprocessor first to prepare the input data) and 
 * output will be sorted by the keys.But it will be a local sort only, while using multiple reducers,
 * the result will be sorted per reducer only.
 */

/**-D mapred.reduce.tasks=30  output/SortDataPreprocessor output/SortByTemperatureUsingHashPartitioner*/

public class SortByTemperatureUsingHashPartitioner extends Configured implements Tool {
	public int run(String[] args) throws Exception {
		Configuration conf = this.getConf();
		Job job = Job.getInstance(conf, "SortByTemperatureUsingHashPartitioner");
		job.setJarByClass(SortByTemperatureUsingHashPartitioner.class);

		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputKeyClass(IntWritable.class);
		//job.setOutputFormatClass(SequenceFileOutputFormat.class);
		//job.setOutputFormatClass(SequenceFileOutputFormat.class);
		//SequenceFileOutputFormat.setCompressOutput(job, true);
		//SequenceFileOutputFormat.setOutputCompressorClass(job, BZip2Codec.class);
		//SequenceFileOutputFormat.setOutputCompressionType(job, CompressionType.BLOCK);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		Path outputPath = new Path(args[1]);
		FileSystem fs = FileSystem.get(new URI(outputPath.toString()), conf);
		fs.delete(outputPath, true);
		FileOutputFormat.setOutputPath(job, outputPath);
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		String log4jConfPath = "log4j.properties";
		PropertyConfigurator.configure(log4jConfPath);
		System.exit(ToolRunner.run(new SortByTemperatureUsingHashPartitioner(), args));
	}
}