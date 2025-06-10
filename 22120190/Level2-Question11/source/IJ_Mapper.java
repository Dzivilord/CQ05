package main.java.InnerJoin;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

public class IJ_Mapper extends Mapper<LongWritable, Text, Text, Text>{
    
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Split the input line into fields
        String[] fields = value.toString().split(" ");

        if (fields.length < 3) {
            return;
        }
        String table= fields[0];
        String keyFood= fields[1];
        String valueOfKey=fields[2];

        context.write(new Text(keyFood),new Text(table + " " + valueOfKey));
    }
}
