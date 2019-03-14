package com.edge.input;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
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
* input/misc/KeyValueTextInput output/KeyValueTextInput
* input/misc/KeyValueTextInputComaSeparated output/KeyValueTextInputComaSeparated
*/

public class KeyValueTextInput extends Configured implements Tool {
	public static void main(String args[]) throws Exception {
		String log4jConfPath = "log4j.properties";
		PropertyConfigurator.configure(log4jConfPath);
		int res = ToolRunner.run(new KeyValueTextInput(), args);
		System.exit(res);
	}

	public int run(String[] args) throws Exception {
		Configuration conf = this.getConf();
		//conf.set("key.value.separator.in.input.line", ",");
		conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", ",");
		
		Job job = Job.getInstance(conf, "WordCountSampleTemplate");
		job.setJarByClass(KeyValueTextInput.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		//job.setMapOutputKeyClass(Text.class);
		//job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		Path outputPath = new Path(args[1]);
		FileSystem fs = FileSystem.get(new URI(outputPath.toString()), conf);
		fs.delete(outputPath, true);
		FileOutputFormat.setOutputPath(job, outputPath);
		return job.waitForCompletion(true) ? 0 : 1;
	}
}

class Map extends Mapper<Text, Text, Text, Text> {
	public void map(Text k1, Text v1, Context context) throws IOException, InterruptedException {
		context.write(k1, v1);
	}
}

class Reduce extends Reducer<Text, Text, Text, Text> {
	public void reduce(Text Key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		String sum = " || ";
		for (Text value : values)
			sum = sum + value.toString() + " || ";
		context.write(Key, new Text(sum));
	}
}
