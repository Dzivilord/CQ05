import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class HistogramReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

    private Map<Integer, Integer> countMap = new HashMap<>();
    private int maxPixelValue = 0;

    @Override
    protected void setup(Context context) {
        // Đọc giá trị N (pixel tối đa) từ cấu hình nếu cần
        maxPixelValue = context.getConfiguration().getInt("max.pixel.value", 255); // mặc định là 255
    }

    @Override
    public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable v : values) {
            sum += v.get();
        }
        countMap.put(key.get(), sum);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        IntWritable outKey = new IntWritable();
        IntWritable outVal = new IntWritable();

        for (int i = 0; i <= maxPixelValue; i++) {
            outKey.set(i);
            outVal.set(countMap.getOrDefault(i, 0));
            context.write(outKey, outVal);
        }
    }
}
