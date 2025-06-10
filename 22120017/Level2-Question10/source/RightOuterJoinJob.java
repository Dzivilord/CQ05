import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class RightOuterJoinJob {
    public static class JoinMapper extends Mapper<Object, Text, Text, Text> {
        private Text joinKey = new Text();
        private Text joinValue = new Text();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()) return;
            String[] parts = line.split("\\s+");
            if (parts.length != 3) return;

            String table = parts[0];
            String k = parts[1];
            String v = parts[2];

            if (table.equals("FoodPrice")) {
                joinKey.set(k);
                joinValue.set("price:" + v);
                context.write(joinKey, joinValue);
            } else if (table.equals("FoodQuantity")) {
                joinKey.set(k);
                joinValue.set("quantity:" + v);
                context.write(joinKey, joinValue);
            }
        }
    }

    public static class JoinReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            List<String> prices = new ArrayList<>();
            List<String> quantities = new ArrayList<>();
            for (Text val : values) {
                String s = val.toString();
                if (s.startsWith("price:")) {
                    prices.add(s.substring(6));
                } else if (s.startsWith("quantity:")) {
                    quantities.add(s.substring(9));
                }
            }
            if (!quantities.isEmpty()) {
                // Right outer join: output with each quantity
                String price = prices.isEmpty() ? "null" : prices.get(0);
                for (String quantity : quantities) {
                    context.write(key, new Text(price + "\t" + quantity));
                }
            }
            // If only FoodPrice but not FoodQuantity, ignore (right outer join)
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: RightOuterJoinJob <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Right Outer Join FoodPrice with FoodQuantity");
        job.setJarByClass(RightOuterJoinJob.class);

        job.setMapperClass(JoinMapper.class);
        job.setReducerClass(JoinReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}