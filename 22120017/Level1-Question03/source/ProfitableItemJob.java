import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.StringJoiner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ProfitableItemJob {

    public static class ItemMapper extends Mapper<Object, Text, Text, Text> {
        private Text item = new Text();
        private Text transaction = new Text();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()) return;
            String[] parts = line.split("\\t", 2);
            if (parts.length == 2) {
                transaction.set(parts[0].trim());
                item.set(parts[1].trim());
                context.write(item, transaction);
            }
        }
    }

    public static class ProfitableReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Set<String> transactions = new HashSet<>();
            for (Text val : values) {
                transactions.add(val.toString());
            }
            if (transactions.size() > 1) {
                StringJoiner joiner = new StringJoiner("\t");
                for (String t : transactions) joiner.add(t);
                context.write(key, new Text(joiner.toString()));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Profitable Items");
        job.setJarByClass(ProfitableItemJob.class);
        job.setMapperClass(ItemMapper.class);
        job.setReducerClass(ProfitableReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));  // Input directory
        FileOutputFormat.setOutputPath(job, new Path(args[1])); // Output directory

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}