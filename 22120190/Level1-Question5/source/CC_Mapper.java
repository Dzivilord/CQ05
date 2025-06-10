package main.java.ClusterCenter;

import java.io.IOException;


import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;


public class CC_Mapper extends Mapper<LongWritable, Text, IntWritable, Text> {
	
    private static final int[] CENTERS={4, 12 ,24};
    
    @Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString().trim();
        String[] parts = line.split("\t");
        if (parts.length != 2) {
            return; // Skip lines that do not have exactly two parts
        }
        String point= parts[0];
        String coordinate= parts[1];
        
        int coord;
        try{
            coord = Integer.parseInt(coordinate);
        } catch (NumberFormatException e) {
            return; 
        }
        int NearestCenter = CENTERS[0];
        int minDistance = Math.abs(coord - CENTERS[0]);

        for(int i=1; i < CENTERS.length; i++) {
            int distance = Math.abs(coord - CENTERS[i]);
            if (distance < minDistance) {
                minDistance = distance;
                NearestCenter = CENTERS[i];
            }
        }

        context.write(new IntWritable(NearestCenter), new Text(point + "\t" + coordinate));
	}
}
