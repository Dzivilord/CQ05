package main.java.DistanceSort;

import java.io.IOException;
import java.lang.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class DS_Runner {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        if (args.length != 3) {
            System.err.println("Usage: DistanceGroupingJob <input path> <output path> <query point>");
            System.exit(-1);
        }
        
        Configuration conf = new Configuration();
        conf.set("query.point", args[2]);


        Job job = Job.getInstance(conf, "Distance Sort");

        job.setJarByClass(DS_Runner.class);
        job.setMapperClass(DS_Mapper.class);
        job.setReducerClass(DS_Reducer.class);


        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
