package com.edge.basic;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.PropertyConfigurator;

//Run As> Run Configurations..>
//Project-> MapReduce,
//MainClass-> com.edge.basic.WordCountNline
// -D LowerLimit=6  -D mapreduce.job.reduces=2 data/wordCount  output/basic/toolRunner <Configuration conf = this.getConf(); this allows to take runtime arg>
public class WordCountToolRunner extends Configured implements Tool {
	/**
	 * The Driver class is implementing Tool interface The Tool interface has three abstract methods which need to be defined
	*/
	
	public static void main(String args[]) throws Exception {
		String log4jConfPath = "log4j.properties";
		PropertyConfigurator.configure(log4jConfPath);
		System.exit(ToolRunner.run(new WordCountToolRunner(), args));
	}
	/**
	 *  Here the run method is called with parameters  – 
	 *  a new Configuration object – this object and its properties will be  shared across all other Classes 
	 *  and methods as we will observe later.  – an instance of the Driver class, to tell the job where to look for next
	 *  String arguments which are passed in the main class from the console  (or parameters). These are usually the 
	 *  input and output paths
	 *
	 */
	

	public int run(String[] args) throws Exception {
		/** Instead of creating a new configuration object, we are retrieving the one created in run method */
		Configuration conf = this.getConf();		
		//Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "WordCountToolRunner");
		//Job job = new Job(conf, "WordCountToolRunner");
		job.setJarByClass(WordCountToolRunner.class);
		job.setMapperClass(Mapp.class);
		job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		//LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		Path outputPath = new Path(args[1]);
		FileSystem fs = FileSystem.get(new URI(outputPath.toString()), conf);
		fs.delete(outputPath, true);
		FileOutputFormat.setOutputPath(job, outputPath);
		return job.waitForCompletion(true) ? 0 : 1;
	}

}

class Mapp extends Mapper <LongWritable, Text, Text, IntWritable> {

    static IntWritable one = new IntWritable(1);
    public void map(LongWritable k1, Text v1, Context context) throws IOException, InterruptedException {

 String words[] = v1.toString().split(",");
 for (String wrod : words)
    context.write(new Text(wrod), one);
}
}
 class Reducee extends Reducer <Text, IntWritable, Text, IntWritable> {

 public void reduce(Text Key, Iterable<IntWritable>  values, Context context)
   throws IOException, InterruptedException {

  int sum = 0;
  int checkValue = Integer.parseInt(context.getConfiguration().get("LowerLimit"));
  // Fetch the value stored in LowerLimit key, convert it into int from String
  for (IntWritable value : values)
   sum += value.get();
  if (sum >= checkValue)
   context.write(Key, new IntWritable(sum));
  // Writing out only the selective records
 }
}
 
