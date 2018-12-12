package com.edge.input;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.PropertyConfigurator;
/**Default separator is Tab
 *
 * */



/**Run As> Run Configurations..>
* Project-> MapReduce,
* MainClass-> com.edge.input.KeyValueTextInput
* data/KeyValueTextInput output/KeyValueTextInput
* data/KeyValueTextInputComaSeparated output/KeyValueTextInputComaSeparated
*/

public class MultipleInput extends Configured implements Tool {
	public static void main(String args[]) throws Exception {
		String log4jConfPath = "log4j.properties";
		PropertyConfigurator.configure(log4jConfPath);
		int res = ToolRunner.run(new MultipleInput(), args);
		System.exit(res);
	}

	public int run(String[] args) throws Exception {
		Configuration conf = this.getConf();
		conf.set("key.value.separator.in.input.line", ",");
		
		
		Job job = Job.getInstance(conf, "MultipleInput");
		job.setJarByClass(MultipleInput.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		//job.setMapOutputKeyClass(Text.class);
		//job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		//job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		//FileInputFormat.addInputPath(job, new Path(args[0]));
		MultipleInputs.addInputPath(job,  new Path(args[0]),TextInputFormat.class, WordCountMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]),KeyValueTextInputFormat.class, MapKeyValueTextInput.class);
		Path outputPath = new Path(args[2]);
		FileSystem fs = FileSystem.get(new URI(outputPath.toString()), conf);
		fs.delete(outputPath, true);
		FileOutputFormat.setOutputPath(job, outputPath);
		return job.waitForCompletion(true) ? 0 : 1;
	}
}

class MapKeyValueTextInput extends Mapper<Text, Text, Text, Text> {
	public void map(Text k1, Text v1, Context context) throws IOException, 		InterruptedException {
		context.write(k1, v1);
	}
}

class WordCountMapper extends Mapper<LongWritable, Text, Text, Text> {	
	
    protected void map(LongWritable k1, Text v1, Context context) throws IOException, InterruptedException { 
    	String val=v1.toString();
    		context.write( new Text(val.split("	")[0]),new Text(val.split("	")[1]));    	   	
    }
}

class Reducee extends Reducer<Text, Text, Text, Text> {
	public void reduce(Text Key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		String sum = " || ";
		for (Text value : values)
			sum = sum + value.toString() + " || ";
		context.write(Key, new Text(sum));
	}
}
