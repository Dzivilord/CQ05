import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class HistogramReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

    private Map<Integer, Integer> countMap = new HashMap<>();
    private int maxPixelValue = Integer.MIN_VALUE;

    // Tính tổng tần suất
    @Override
    public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        int pixel = key.get();
        int sum = 0;
        for (IntWritable v : values) {
            sum += v.get();
        }
        countMap.put(pixel, sum);

        // Cập nhật giá trị pixel lớn nhất gặp được
        if (pixel > maxPixelValue) {
            maxPixelValue = pixel;
        }
    }

    // Những pixel không xuất hiện cũng được thể hiện tần suất (từ 0 đến max pixel value)
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
