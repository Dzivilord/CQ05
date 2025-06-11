import java.io.IOException;
import java.util.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;


public class MergeMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            String[] parts = line.split("\t");
            if (parts.length == 2) {
                context.write(new Text(parts[0]), new IntWritable(Integer.parseInt(parts[1])));
            }
        }
    }