package com.edge.F_customPartitioner;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class LetterCasePartition {
    public static void main(String[] args) throws Exception { 
    	
    	Configuration conf = new Configuration();
		if (args.length != 2) {
			System.err.println("Usage: wordcount <in> <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "word count");
		job.setJarByClass(LetterCasePartition.class);
		job.setMapperClass(WordCountMapper.class);
		job.setCombinerClass(WordCountReducer.class);
		job.setReducerClass(WordCountReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setNumReduceTasks(2);
		job.setPartitionerClass(LetterCasePartitioner.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
    
}

class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	

	private final Text key =  new Text();    	
	private IntWritable val  =  new IntWritable(1);	
    protected void map(LongWritable bos, Text line, Context context) throws IOException, InterruptedException {    	
    	StringTokenizer st=new StringTokenizer(line.toString());
    	while (st.hasMoreTokens()){ 
    		key.set(st.nextToken());
    		context.write(key, val);
    	}    	
    }
}



class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	private IntWritable sum1 = new IntWritable();

	protected void reduce(Text key, Iterable<IntWritable> val, Context context)
			throws IOException, InterruptedException {

		int sum = 0;
		for (IntWritable value : val) {
			sum += value.get();
		}
		sum1.set(sum);
		context.write(key, sum1);
	}

}


 class LetterCasePartitioner extends Partitioner<Text, IntWritable> {

	
	public int getPartition(Text key, IntWritable value, int numPart) {
		if (numPart == 2) {
			if (Character.isLowerCase(key.charAt(0))) {
				return 0;
			} else {
				return 1;
			}
		} else {
			System.err
					.println("WordCountParitioner can only handle either 1 or 2 paritions");
			return 1;
		}
	}

}

