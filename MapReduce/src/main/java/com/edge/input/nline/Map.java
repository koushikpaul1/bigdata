package com.edge.input.nline;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

class Map extends Mapper<LongWritable, Text,IntWritable, Text> {
	int i=0;
    protected void map(LongWritable k1, Text v1, Context context) throws IOException, InterruptedException {    	
    	 IntWritable k2= new IntWritable(++i);    		
    		context.write(k2, v1);    	    	
    }
}