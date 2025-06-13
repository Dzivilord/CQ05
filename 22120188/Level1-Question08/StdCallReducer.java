import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class StdCallReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private IntWritable result = new IntWritable();
    
    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        int totalDuration = 0;

        for (IntWritable val : values) {
            totalDuration += val.get();
        }
        // Tính các tổng thời gian gọi, nếu tổng thời gian lớn hơn 60 phút thì ghi kết quả
        if (totalDuration > 60) {
            result.set(totalDuration);
            context.write(key, result);
        }
    }
}
