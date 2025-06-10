import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.Duration;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class StdCallMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    // Date format: "yyyy-MM-dd HH:mm:ss" Xác định kiểu dữ liệu thời gian để tính khoảng thời gian
    private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private Text phoneNumber = new Text();
    private IntWritable durationWritable = new IntWritable();

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        try {
            String[] parts = value.toString().split("\\|");
            if (parts.length != 5) return;

            // Tách các phần bằng |, giá trị ToPhoneNumber không quan trọng
            String fromPhone = parts[0];
            String callStart = parts[2];
            String callEnd = parts[3];
            String stdFlag = parts[4];
            
            // Bỏ qua dòng nếu stdFlag không phải là "1"
            if (!stdFlag.trim().equals("1")) return;

            // Tính khoảng thời gian cuộc gọi
            LocalDateTime start = LocalDateTime.parse(callStart.trim(), formatter);
            LocalDateTime end = LocalDateTime.parse(callEnd.trim(), formatter);
            long durationMinutes = Duration.between(start, end).toMinutes();

            phoneNumber.set(fromPhone);
            durationWritable.set((int) durationMinutes);
            context.write(phoneNumber, durationWritable);
        } catch (Exception e) {
            
        }
    }
}
