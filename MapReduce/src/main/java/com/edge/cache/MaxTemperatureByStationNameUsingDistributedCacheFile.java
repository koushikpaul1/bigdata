package com.edge.cache;

//MaxTemperatureByStationNameUsingDistributedCacheFile Application to find the maximum temperature by station, showing station names from a lookup table passed as a distributed cache file
import java.io.File;
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
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.PropertyConfigurator;

import com.edge.basic.WordCountToolRunner;
import com.edge.util.book.NcdcRecordParser;
import com.edge.util.book.NcdcStationMetadata;

/** The program finds the maximum temperature by weather station, so the mapper (StationTemperatureMapper) simply emits (station ID, temperature) pairs. 
 * For the combiner,  MaxTemperatureReducer is used to pick the maximum temperature for any given group of map outputs on the map side. The reducer
 *  (MaxTemperatureReducerWithStationLookup) is different from the combiner, since in addition to finding the maximum temperature, it uses the cache 
 *  file to look up the station name. 
 */

/**
 * -files data/ncdc/metadata/stations-fixed-width.txt data/ncdc/NCDC
 * output/distributedCache
 */
public class MaxTemperatureByStationNameUsingDistributedCacheFile extends Configured implements Tool {
	public int run(String[] args) throws Exception {
		Configuration conf = this.getConf();
		Job job = Job.getInstance(conf, "MaxTemperatureByStationNameUsingDistributedCacheFile");
		job.setJarByClass(WordCountToolRunner.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		Path outputPath = new Path(args[1]);
		FileSystem fs = FileSystem.get(new URI(outputPath.toString()), conf);
		fs.delete(outputPath, true);
		FileOutputFormat.setOutputPath(job, outputPath);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setMapperClass(StationTemperatureMapper.class);
		job.setCombinerClass(MaxTemperatureReducer.class);
		job.setReducerClass(MaxTemperatureReducerWithStationLookup.class);
		//job.addCacheFile(new URI("data/ncdc/metadata/stations-fixed-width.txt"));// supplied at runtime -files data/ncdc/metadata/stations-fixed-width.txt
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		String log4jConfPath = "log4j.properties";
		PropertyConfigurator.configure(log4jConfPath);
		System.exit(ToolRunner.run(new MaxTemperatureByStationNameUsingDistributedCacheFile(), args));
	}
}

class StationTemperatureMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	private NcdcRecordParser parser = new NcdcRecordParser();

	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		parser.parse(value);
		if (parser.isValidTemperature()) {
			context.write(new Text(parser.getStationId()), new IntWritable(parser.getAirTemperature()));
		}
	}
}

class MaxTemperatureReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		int maxValue = Integer.MIN_VALUE;
		for (IntWritable value : values) {
			maxValue = Math.max(maxValue, value.get());
		}
		context.write(key, new IntWritable(maxValue));
	}
}

class MaxTemperatureReducerWithStationLookup extends Reducer<Text, IntWritable, Text, IntWritable> {
	private NcdcStationMetadata metadata;

	protected void setup(Context context) throws IOException, InterruptedException {
		metadata = new NcdcStationMetadata();
		metadata.initialize(new File("stations-fixed-width.txt"));
	}

	protected void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		String stationName = metadata.getStationName(key.toString());
		int maxValue = Integer.MIN_VALUE;
		for (IntWritable value : values) {
			maxValue = Math.max(maxValue, value.get());
		}
		context.write(new Text(stationName), new IntWritable(maxValue));
	}
}