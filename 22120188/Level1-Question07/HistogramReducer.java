import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class HistogramReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

    private Map<Integer, Integer> countMap = new HashMap<>();
<<<<<<< HEAD
    private int maxPixelValue = Integer.MIN_VALUE;

    // Tính tổng tần suất
    @Override
    public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        int pixel = key.get();
=======
    private int maxPixelValue = 0;

    @Override
    protected void setup(Context context) {
        // Đọc giá trị N (pixel tối đa) từ cấu hình nếu cần
        maxPixelValue = context.getConfiguration().getInt("max.pixel.value", 255); // mặc định là 255
    }

    @Override
    public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
>>>>>>> c44f54b21e13924bf0d05f751f8fd7911e8c9ea9
        int sum = 0;
        for (IntWritable v : values) {
            sum += v.get();
        }
<<<<<<< HEAD
        countMap.put(pixel, sum);

        // Cập nhật giá trị pixel lớn nhất gặp được
        if (pixel > maxPixelValue) {
            maxPixelValue = pixel;
        }
    }

    // Những pixel không xuất hiện cũng được thể hiện tần suất (từ 0 đến max pixel value)
=======
        countMap.put(key.get(), sum);
    }

>>>>>>> c44f54b21e13924bf0d05f751f8fd7911e8c9ea9
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
