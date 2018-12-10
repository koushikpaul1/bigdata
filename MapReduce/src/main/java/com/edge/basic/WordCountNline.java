package com.edge.basic;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.PropertyConfigurator;
//Run As> Run Configurations..>
//Project-> MapReduce,
//MainClass-> com.edge.basic.WordCountNline
//data/wordCount  output/WordCountNline 
public class WordCountNline {
    public static void main(String[] args) throws Exception { 
    	
    	String log4jConfPath = "log4j.properties";
		PropertyConfigurator.configure(log4jConfPath);
    	Configuration conf = new Configuration();
		if (args.length != 2) {
			System.err.println("Usage: wordcount <in> <out>");
			System.exit(2);
		}
		Job job = Job.getInstance(conf, "word count");
		
		job.setJarByClass(WordCountNline.class);
		job.setMapperClass(Map.class);
		job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		Path outputPath = new Path(args[1]);
		FileSystem fs = FileSystem.get(new URI(outputPath.toString()), conf);
		fs.delete(outputPath,true);
		
        job.setInputFormatClass(NLineInputFormat.class);
        NLineInputFormat.addInputPath(job, new Path(args[0]));
        job.getConfiguration().setInt("mapreduce.input.lineinputformat.linespermap", 4);

        LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

		
	
		
		
		//FileInputFormat.addInputPath(job, new Path(args[0]));
		//FileOutputFormat.setOutputPath(job, outputPath);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
    
}



