import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class OrderCombiner extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        String price = null;
        String quantity = null;

        // Duyệt qua tất cả các giá trị nhận được
        for (Text val : values) {
            String[] parts = val.toString().split("\\|");
            if (parts.length != 2) continue;

            String tag = parts[0];
            String data = parts[1];

            if (tag.equals("P") && price == null) {
                price = "P|" + data;
            } else if (tag.equals("Q") && quantity == null) {
                quantity = "Q|" + data;
            }
        }

        // Gộp thành 1 dòng
        if (price != null || quantity != null) {
            StringBuilder combined = new StringBuilder();
            if (price != null) combined.append(price);
            if (quantity != null) {
                if (combined.length() > 0) combined.append(" ");
                combined.append(quantity);
            }
            context.write(key, new Text(combined.toString()));
        }
    }
}
