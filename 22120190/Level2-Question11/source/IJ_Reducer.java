package main.java.InnerJoin;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

public class IJ_Reducer extends Reducer<Text, Text, Text, NullWritable> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuilder result = new StringBuilder();
        
        String price=null;
        String quantity=null;

        for (Text value : values){
            String[] parts = value.toString().split(" ");
            if (parts.length < 2) {
                continue;
            }
            String table = parts[0];
            String data = parts[1];

            if (table.equals("FoodPrice")) {
                price = data;
            } else if (table.equals("FoodQuantity")) {
                quantity = data;
            }
        }
        if (price != null && quantity != null) {
            result.append(key.toString()).append(" ").append(price).append(" ").append(quantity);
            context.write(new Text(result.toString()), NullWritable.get());
        }
    }
    
}
