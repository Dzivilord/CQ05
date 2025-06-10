import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

public class OrderMapper extends Mapper<LongWritable, Text, Text, Text> {
    private Text joinKey = new Text();
    private Text joinValue = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String[] parts = value.toString().trim().split("\\s+");
        if (parts.length != 3) return;

        String table = parts[0];
        String item = parts[1];
        String val = parts[2];

        joinKey.set(item);
        if (table.equals("FoodPrice")) {
            joinValue.set("P|" + val); // Tag for price
        } else if (table.equals("FoodQuantity")) {
            joinValue.set("Q|" + val); // Tag for quantity
        } else {
            return; // invalid table
        }

        context.write(joinKey, joinValue);
    }
}
