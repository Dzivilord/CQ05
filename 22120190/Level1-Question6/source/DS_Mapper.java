package main.java.DistanceSort;

import java.io.IOException;
import java.lang.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

public class DS_Mapper extends Mapper<LongWritable, Text, IntWritable, Text> {

    private int queryPoint;

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
        
        try{
            queryPoint = Integer.parseInt(context.getConfiguration().get("query.point"));
        } catch (NumberFormatException e) {
            throw new IOException("Invalid query point configuration", e);
        }
    }

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString().trim();
        String[] parts = line.split("\t");
        if (parts.length != 2) {
            return; 
        }
        String point = parts[0];
        String coordinate = parts[1];

        int coord;
        try {
            coord = Integer.parseInt(coordinate);
        } catch (NumberFormatException e) {
            return; 
        }

        int distance = Math.abs(coord - queryPoint);
        context.write(new IntWritable(distance), new Text(point));
    }
}
