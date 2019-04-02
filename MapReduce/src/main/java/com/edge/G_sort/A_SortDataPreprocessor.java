package com.edge.G_sort;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.PropertyConfigurator;

import com.edge.util.book.NcdcRecordParser;

/**Run As> Run Configurations..>
 * Project-> MapReduce,
 * MainClass-> com.edge.basic.WordCountNline
 *  input/ncdc/NCDC output/SortDataPreprocessor*/

public class A_SortDataPreprocessor extends Configured implements Tool {
	/**
	 * Storing temperatures as Text objects doesn’t work for sorting purposes,
	 * because signed integers don’t sort lexicographically. Instead, we are
	 * going to store the data using sequence files whose IntWritable keys represent
	 * the temperatures (and sort correctly) and whose Text values are the lines of
	 * data.
	 */
	public static void main(String args[]) throws Exception {
		String log4jConfPath = "log4j.properties";
		PropertyConfigurator.configure(log4jConfPath);
		System.exit(ToolRunner.run(new A_SortDataPreprocessor(), args));
	}

	public int run(String[] args) throws Exception {
		
		Configuration conf = this.getConf();
		Job job = Job.getInstance(conf, "SortDataPreprocessor");
		job.setMapperClass(CleanerMapper.class);
		job.setNumReduceTasks(0);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		SequenceFileOutputFormat.setCompressOutput(job, true);
		SequenceFileOutputFormat.setOutputCompressorClass(job, BZip2Codec.class);
		SequenceFileOutputFormat.setOutputCompressionType(job, CompressionType.BLOCK);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		Path outputPath = new Path(args[1]);
		FileSystem fs = FileSystem.get(new URI(outputPath.toString()), conf);
		fs.delete(outputPath, true);
		FileOutputFormat.setOutputPath(job, outputPath);
		return job.waitForCompletion(true) ? 0 : 1;
	}

}

class CleanerMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
	private NcdcRecordParser parser = new NcdcRecordParser();

	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		parser.parse(value);
		if (parser.isValidTemperature()) {
			context.write(new IntWritable(parser.getAirTemperature()), value);
		}
	}
}
