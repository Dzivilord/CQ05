import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

public class OrderMapper extends Mapper<LongWritable, Text, Text, Text> {
    private Text joinKey = new Text();
    private Text joinValue = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        // Tách từng phần bằng khoảng trống
        String[] parts = value.toString().trim().split("\\s+");
        if (parts.length != 3) return;
        // FoodPrice Pizza 8 
        // Xác định từng thành phần : Price/Quantity- FoodName - Value
        String tag = parts[0];
        String item = parts[1];
        String val = parts[2];

        // Kiểm tra xem table có phải là FoodPrice hoặc FoodQuantity không, đính kèm thông tin để nhận biết là giá hay là số lượng 
        joinKey.set(item);
        if (tag.equals("FoodPrice")) {
            joinValue.set("P|" + val); 
        } else if (tag.equals("FoodQuantity")) {
            joinValue.set("Q|" + val); 
        } else {
            return; 
        }

        context.write(joinKey, joinValue);
    }
}
