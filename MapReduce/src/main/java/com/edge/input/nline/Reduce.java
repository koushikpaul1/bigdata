package com.edge.input.nline;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

class Reduce extends Reducer<IntWritable,Text, IntWritable,Text > {

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