package com.edge.counter;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;



class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	
	enum WordCount {COUNT}
	private final Text k2 =  new Text();    	
	private IntWritable v2  =  new IntWritable(1);	
    protected void map(LongWritable k1, Text v1, Context context) throws IOException, InterruptedException {    	
    	StringTokenizer st=new StringTokenizer(v1.toString());
    	context.getCounter(WordCount.COUNT).increment(1);  
    	while (st.hasMoreTokens()){ 
    		String key=st.nextToken();
    			if(key=="Baskerville")System.err.println("Found Baskerville: " + key);
    		k2.set(key);
    		context.write(k2, v2);
    	}    	
    }
}