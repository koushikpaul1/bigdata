package com.edge.toolRunner;

import java.io.IOException;
import java.net.URI;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.PropertyConfigurator;

import com.edge.fileMerge.sequenceFile.WholeFileInputFormat;

public class WordCountTemplate extends Configured implements Tool {
	public static void main(String args[]) throws Exception {
		String log4jConfPath = "log4j.properties";
		PropertyConfigurator.configure(log4jConfPath);
		System.exit(ToolRunner.run(new WordCountTemplate(), args));
	}

	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf, "WordCountTemplate");
		job.setJarByClass(WordCountTemplate.class);
		job.setMapperClass(WordCountTemplateMapper.class);
		// job.setCombinerClass(WordCountTemplateReducer.class);
		job.setReducerClass(WordCountTemplateReducer.class);

		// job.setMapOutputKeyClass(Text.class);
		// job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		// job.setInputFormatClass(TextInputFormat.class);//Default
		// job.setOutputFormatClass(TextOutputFormat.class);//Default
		// job.setInputFormatClass(WholeFileInputFormat.class);
		// job.setOutputFormatClass(SequenceFileOutputFormat.class);
		// LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		Path outputPath = new Path(args[1]);
		FileSystem fs = FileSystem.get(new URI(outputPath.toString()), conf);
		fs.delete(outputPath);
		FileOutputFormat.setOutputPath(job, outputPath);
		return job.waitForCompletion(true) ? 0 : 1;
	}

}

class WordCountTemplateMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	private final Text k2 = new Text();
	private IntWritable v2 = new IntWritable(1);
	protected void setup(Context context) {}
	protected void map(LongWritable k1, Text v1, Context context) throws IOException, InterruptedException {
		StringTokenizer st = new StringTokenizer(v1.toString());
		while (st.hasMoreTokens()) {
			k2.set(st.nextToken());
			context.write(k2, v2);
		}
	}
	protected void cleanup(Context context) {}
}

class WordCountTemplateReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	private IntWritable sum1 = new IntWritable();
	protected void setup(Context context) {}
	protected void reduce(Text k3, Iterable<IntWritable> v3, Context context) throws IOException, InterruptedException {
		int sum = 0;
		for (IntWritable value : v3) {
			sum += value.get();
		}
		sum1.set(sum);
		context.write(k3, sum1);
	}
	protected void cleanup(Context context) {}
	}




