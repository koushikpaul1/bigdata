package com.edge.basic;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

class MapNline extends Mapper<LongWritable, Text, IntWritable, Text> {
	

	private final IntWritable k2 =  new IntWritable(0);    	
	int i;
    protected void map(LongWritable k1, Text v1, Context context) throws IOException, InterruptedException {  
    		k2.set(++i);String val=v1+"\n*****************";Text value=new Text();value.set(val);
    		context.write(k2, value);
    	    	
    }
}