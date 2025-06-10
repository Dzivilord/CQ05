package main.java.DistanceSort;

import java.io.IOException;
import java.lang.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

public class DS_Reducer extends Reducer<IntWritable,Text,IntWritable,Text> {

    @Override
    public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuilder result = new StringBuilder();
        
        for (Text value : values) {
            if (result.length() > 0) {
                result.append(" "); // Add space between points
            }
            result.append(value.toString());
        }
        context.write(key, new Text(result.toString()));
    }
    
}
