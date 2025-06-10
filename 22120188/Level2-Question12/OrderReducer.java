import java.io.IOException;
import java.util.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

public class OrderReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        String price = "null";
        String quantity = "null";

        // values = ['P|8', 'Q|3']
        // values = ['P|8', 'Q|3']     
        for (Text val : values) {
            String[] parts = val.toString().split("\\|");
            if (parts[0].equals("P")) {
                price = parts[1];
            } else if (parts[0].equals("Q")) {
                quantity = parts[1];
            }
        }

        context.write(key, new Text(price + "\t" + quantity));
    }
}
