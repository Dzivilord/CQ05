import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class HistogramMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private IntWritable pixelValue = new IntWritable();

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString().trim();
        if (line.isEmpty()) return;

        String[] pixels = line.split("\\s+");
        for (String p : pixels) {
            try {
                int val = Integer.parseInt(p);
                pixelValue.set(val);
                context.write(pixelValue, one);
            } catch (NumberFormatException e) {
                // Bỏ qua nếu không phải số
            }
        }
    }
}
