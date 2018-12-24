package com.edge.sort;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.PropertyConfigurator;
/**
 * This program's input is a sequencefile with intwritable keys (run
 * SortDataPreprocessor first to prepare the input data) and 
 * output will be sorted by the keys. And it will be a global sort, It has two steps 
 * Step1: It takes the input and using RandomSampler it samples the input in moderately even samples, 
 * then it upload this as input for the next phase using distributed cache .
 */
/**
 * -D mapred.reduce.tasks=30 data/seqenceFilewithLongwritableKey
 * output/SortByTemperatureUsingTotalOrderPartitioner
 */
public class SortByTemperatureUsingTotalOrderPartitioner extends Configured implements Tool {
	public int run(String[] args) throws Exception {
		
		Configuration conf = getConf();
		Job job = Job.getInstance(conf, "SortByTemperatureUsingTotalOrderPartitioner");
		job.setJarByClass(SortByTemperatureUsingTotalOrderPartitioner.class);
		Path outputPath = new Path(args[1]);
		FileSystem fs = FileSystem.get(new URI(outputPath.toString()), conf);
		fs.delete(outputPath, true);
		FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputKeyClass(IntWritable.class);
		// job.setOutputFormatClass(SequenceFileOutputFormat.class);
		// SequenceFileOutputFormat.setCompressOutput(job, true);
		// SequenceFileOutputFormat.setOutputCompressorClass(job, BZip2Codec.class);
		// SequenceFileOutputFormat.setOutputCompressionType(job,
		// CompressionType.BLOCK);		
		// job.setPartitionerClass(HashPartitioner.class);
		job.setPartitionerClass(TotalOrderPartitioner.class);
		InputSampler.Sampler<IntWritable, Text> sampler = new InputSampler.RandomSampler<IntWritable, Text>(0.1, 10000,10);
		InputSampler.writePartitionFile(job, sampler);
		
		

		// Add to DistributedCache	
		String partitionFile = TotalOrderPartitioner.getPartitionFile(conf);
		URI partitionUri = new URI(partitionFile);
		job.addCacheFile(partitionUri);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		String log4jConfPath = "log4j.properties";
		PropertyConfigurator.configure(log4jConfPath);
		int exitCode = ToolRunner.run(new SortByTemperatureUsingTotalOrderPartitioner(), args);
		System.exit(exitCode);
	}
}
