package com.edge.input.nline;

/*With TextInputFormat and KeyValueTextInputFormat, each mapper receives a variable
number of lines of input. The number depends on the size of the split and the length of the
lines. If you want your mappers to receive a fixed number of lines of input, then
NLineInputFormat is the InputFormat to use.*/
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class NLine extends Configured implements Tool {

    
    public int run(String[] args) throws Exception {

        if (args.length != 2) {
            System.out
                  .printf("Two parameters are required for NLineNLineInputFormat- <input dir> <output dir>\n");
            return -1;
        }

        Job job = new Job(getConf());
        job.setJobName("NLineInputFormat example");
        job.setJarByClass(NLine.class);

        job.setInputFormatClass(NLineInputFormat.class);
        NLineInputFormat.addInputPath(job, new Path(args[0]));
        job.getConfiguration().setInt("mapreduce.input.lineinputformat.linespermap", 4);

        LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(MapperNLine.class);
        job.setNumReduceTasks(0);

        boolean success = job.waitForCompletion(true);
        return success ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Configuration(), new NLine(), args);
        System.exit(exitCode);
    }
}


class MapperNLine extends Mapper<LongWritable, Text, LongWritable, Text> {

    @Override
    public void map(LongWritable key, Text value, Context context)
          throws IOException, InterruptedException {

        context.write(key, value);
    }

}