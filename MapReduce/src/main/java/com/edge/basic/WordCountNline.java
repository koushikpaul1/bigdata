package com.edge.basic;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.PropertyConfigurator;

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
		job.setMapperClass(MapNline.class);
		//job.setCombinerClass(ReduceNline.class);
		job.setReducerClass(ReduceNline.class);
		
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		
		Path outputPath = new Path(args[1]);
		FileSystem fs = FileSystem.get(new URI(outputPath.toString()), conf);
		fs.delete(outputPath,true);
		
        job.setInputFormatClass(NLineInputFormat.class);
        NLineInputFormat.addInputPath(job, new Path(args[0]));
        job.getConfiguration().setInt("mapreduce.input.lineinputformat.linespermap", 1000);
        /**
		 * FileOutputFormat subclasses will create output (part-r-nnnnn) files, even if
		 * they are empty. Some applications prefer that empty files not be created,
		 * which is where LazyOutputFormat helps. It is a wrapper output format that
		 * ensures that the output file is created only when the first record is emitted
		 * for a given partition.
		 */
        //job.setOutputFormatClass(TextOutputFormat.class);
		LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job,outputPath);

		
	
		
		
		//FileInputFormat.addInputPath(job, new Path(args[0]));
		//FileOutputFormat.setOutputPath(job, outputPath);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
    
}
class MapNline extends Mapper<LongWritable, Text, IntWritable, Text> {
	

	private final IntWritable k2 =  new IntWritable(0);    	
	int i;
    protected void map(LongWritable k1, Text v1, Context context) throws IOException, InterruptedException {  
    		k2.set(++i);String val=v1+"\n*****************";Text value=new Text();value.set(val);
    		context.write(k2, value);
    	    	
    }
}
class ReduceNline extends Reducer<IntWritable,Text, IntWritable,Text > {

	private String sum = new String();

	protected void reduce(IntWritable k3, Iterable<Text> v3, Context context)
			throws IOException, InterruptedException {
		
		for (Text value : v3) {
			sum += value+"\n";
		} sum+="\n\n\n===================================\n";
		Text value3  =  new Text();value3.set(sum);	
		context.write(k3,value3 );
	}

}

