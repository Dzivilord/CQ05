import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
public class OrderReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        String price = "null";
        String quantity = "null";

        for (Text val : values) {
            // val có thể là "P|8", 'Q|3' hoặc "P|8 Q|3"
            String[] tokens = val.toString().split("\\s+");
            for (String token : tokens) {
                String[] parts = token.split("\\|");
                if (parts.length == 2) {
                    if (parts[0].equals("P")) {
                        price = parts[1];
                    } else if (parts[0].equals("Q")) {
                        quantity = parts[1];
                    }
                }
            }
        }
        context.write(key, new Text(price + "\t" + quantity));
    }
}
