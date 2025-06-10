package main.java.InnerJoin;

import java.io.IOException;

import org.apache.hadoop.fs.Path;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class IJ_Runner {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        if (args.length != 2) {
            System.err.println("Usage: Inner Join <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Inner Join");

        job.setJarByClass(IJ_Runner.class);
        job.setMapperClass(IJ_Mapper.class);
        job.setReducerClass(IJ_Reducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
