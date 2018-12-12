package com.edge.output.multipleOutputs;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**Run As> Run Configurations..>
* Project-> MapReduce,
* MainClass-> com.edge.output.multipleOutputs.MultipleOutputsPartitionByStationUsing
* data/NCDC output/NCDC/
*/


public class MultipleOutputsPartitionByStationUsing extends Configured implements Tool {
	static class StationMapper extends Mapper<LongWritable, Text, Text, Text> {
		private NcdcRecordParser parser = new NcdcRecordParser();

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			parser.parse(value);
			context.write(new Text(parser.getStationId()), value);
		}
	}

	static class MultipleOutputsReducer extends Reducer<Text, Text, Text, Text> {
		private MultipleOutputs<Text, Text> multipleOutputs;
		private NcdcRecordParser parser = new NcdcRecordParser();

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			multipleOutputs = new MultipleOutputs<Text, Text>(context);
		}

		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			for (Text value : values) {
				parser.parse(value);
				String val = parser.getObservationDate() + " Temp=" + parser.getAirTemperature() + " TempString="
						+ parser.getAirTemperatureString() + " Quality=" + parser.getQuality();
				multipleOutputs.write(new Text(parser.getYear()), new Text(val), key.toString());
			}
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			multipleOutputs.close();
		}
	}

	public int run(String[] args) throws Exception {
		Configuration conf = this.getConf();
		Job job = Job.getInstance(conf, "MultipleOutputsPartitionByStationUsing");
		if (job == null) {
			return -1;
		}
		job.setMapperClass(StationMapper.class);
		job.setReducerClass(MultipleOutputsReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		Path outputPath = new Path(args[1]);
		FileSystem fs = FileSystem.get(new URI(outputPath.toString()), conf);
		fs.delete(outputPath);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, outputPath);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new MultipleOutputsPartitionByStationUsing(), args);
		System.exit(exitCode);
	}
}