package com.edge.F_customPartitioner;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper;


/*
 * Case of Poor partitioning and how to overcome it ??
Suppose you know that one of the key in your data input will appear more than
 any other key so you may want to send all your key (large number) to one partition 
 and then distribute the other keys over all other partition by their hasCode().
  So now if you have two mechanism of sending data to partitions
 
First,the key appearing more will be send to one partition
Second, all other keys will be send to partitions according to their hashCode().
Now suppose if your hashCode() method does not uniformly distribute other keys data over partitions range.
 So the data is not evenly distributed in partitions as well as reducers.Since each partition is equivalent
  to a reducer.So here some reducers will have more data than other reducers.So other reducers will wait for 
  one reducer(one with user defined keys) due to the work load it shares.
So here we should take an approach that its work load may be shared across many different reducers.
Example Scenario
Let's say i have an data set of the form person information,Our dataset contains the name,country,
 sports liked.
PersonA,India,Cricket
PersonB,Brazil,Soccer
PersonC,Australia,Baseball
PersonD,India,Cricket
PersonE,England,Cricket
PersonF,Australia,Cricket
PersonG,India,Cricket
PersonH,England,Cricket
PersonI,India,Cricket
PersonJ,India,Cricket
PersonK,India,Cricket
..
We need to count the person for each of the game in the list. 
So our key becomes the third field i.e., the game . 
Observe the above example and let's suppose we have a large set of data like this where the frequency
 of data is in direction of country india.
And what does the default partitioner does it will send out all values with the same keys to same reducer.
So all values with same key(cricket) send to same reducer. but since our data contains a large number of
 such key value pair due the fact the country frequency of dataset is india.
And also data key (cricket) is also present for other country map output. So we have a lots of key value
data to send to same reducer for cricket key. Here we will write our custom partitioner. And follows the approach below. 
Our custom partitioner will send all key value by country india to one partition and other key value with countries 
like(England,Australia) to other partition so that work load one reducer that should process key cricket is divided into two reducers
  */


public class Sports extends Configured implements Tool {

	
	
	public static void main(String[] args) throws Exception {
		int res= ToolRunner.run(new Configuration(),new Sports(),args);
		System.exit(res);
	}
	public int run(String[] args) throws Exception {
		if(args.length!=2){
			System.out.print("Run as --> hadoop jar /path/to/name.jar /inputdataset /output");
			System.exit(-1);
		}
		
		Configuration conf = this.getConf();
		//Job job = Job.getInstance(conf);
		Job job = new Job(conf, "PartitionerDemo");
		job.setJarByClass(Sports.class);
		
		//Set number of reducer tasks
		job.setNumReduceTasks(3);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setMapperClass(PartitionerMap.class);
		job.setReducerClass(PartitionerReduce.class);

		//Set Partitioner Class
		job.setPartitionerClass(customPartitioner.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		return job.waitForCompletion(true) ? 0 : 1;
	}
	
}


/**
 * Mapper class generating key value pair of game,country as intermediate keys
 */
 class PartitionerMap extends Mapper<LongWritable, Text, Text, Text> {
	public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException {
		String[] words = value.toString().split(" ");
		try{
		context.write(new Text(words[2]),new Text(words[1]));
		}
		catch(Exception e){
			System.err.println(e);
		}
	}
}
/**
 * Each partition processed by different reducer tasks as defined in our custom partitioner
 */


class PartitionerReduce extends Reducer<Text,Text,Text,IntWritable> {
	public void reduce(Text key,Iterable<Text>  values,Context context) throws IOException, InterruptedException {
		int gameCount=0;
		for(Text val:values){
			gameCount++;
		}
		context.write(new Text(key),new IntWritable(gameCount));
	}
}


/**
 * Our custom Partitioner class will divide the dataset into three partitions one with key as cricket and value as
 * india, second partition with key as cricket and value other than india, and third partition with game(key) other 
 * than cricket 
 */

 class customPartitioner extends Partitioner<Text,Text>{
	public int getPartition(Text key, Text value, int numReduceTasks){
	if(numReduceTasks==0)
		return 0;
	if(key.equals(new Text("Cricket")) && !value.equals(new Text("India")))
		return 0;
	if(key.equals(new Text("Cricket")) && value.equals(new Text("India")))
		return 1;
	else
		return 2;
	}
 }
 