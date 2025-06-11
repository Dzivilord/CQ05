import java.io.IOException;
import java.util.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

public class ItemSetkReducer extends Reducer<Text,IntWritable, Text,IntWritable>{
    private double minSupport;
    private int totalTransactions;

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
        minSupport = context.getConfiguration().getDouble("minSupport", 0.1);
        totalTransactions = context.getConfiguration().getInt("totalTransactions", 1);
    }

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable value : values) {
            sum += value.get();
        }
        
        if (sum > 0) {
            double support = (double) sum / totalTransactions;
            if (support >= minSupport) {
                context.write(key, new IntWritable(sum));
            }
        }
    }
}