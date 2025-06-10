package main.java.ClusterCenter;


import java.io.IOException;
import java.util.List;
import java.util.ArrayList;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

public class CC_Reducer extends Reducer<IntWritable,Text, Text,NullWritable>{

    @Override
    public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        List<String> points = new ArrayList<>();
        List<Integer> coordinates = new ArrayList<>();

        for(Text value:values){
            String[] parts = value.toString().split("\t");
            if (parts.length != 2) {
                continue; // Skip lines that do not have exactly two parts
            }
            int coord= Integer.parseInt(parts[1]);
            String point = parts[0];
            points.add(point);
            coordinates.add(coord);
            }
        double sum = 0.0;
        for (int coord : coordinates) {
            sum += coord;
        }
        if (coordinates.isEmpty()) {
            return; // Avoid division by zero
        }
        double mean= sum / coordinates.size();

        StringBuilder result = new StringBuilder();
        result.append(key.get()).append("\t").append(mean).append("\t");

        for (String point : points) {
            result.append(point).append(" ");
        }

        context.write(new Text(result.toString().trim()), NullWritable.get());
    }
}
