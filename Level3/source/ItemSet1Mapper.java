import java.io.IOException;
import java.util.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

public class ItemSet1Mapper extends Mapper<LongWritable, Text, Text, IntWritable>{
    private final static IntWritable one = new IntWritable(1);
    private final Text totalKey= new Text("TOTAL");

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line= value.toString().trim();
        String[] parts = line.split("\t");
        if (parts.length<2){
            return;
        }
        String[] items=parts[1].split(" ");
        for (String item : items) {
            if (!item.isEmpty()) {
                context.write(new Text(item), one);
            }
        }
    }
}