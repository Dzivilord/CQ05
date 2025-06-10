import java.io.IOException;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Ques_one {

    public static class Ques_oneMapper extends Mapper<LongWritable, Text, Text, Text> {
        private Text ID = new Text();
        private Text item = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts_of_input = value.toString().split("\t");
            if (parts_of_input.length == 2) {
                ID.set(parts_of_input[0].trim());
                item.set(parts_of_input[1].trim());
                context.write(ID, item);
            }
        }
        
    }

    public static class Ques_oneReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Map<String, Integer> itemCounts = new HashMap<>();

            for (Text val : values) {
                String item = val.toString();
                itemCounts.put(item, itemCounts.getOrDefault(item, 0) + 1);
            }

            List<Map.Entry<String, Integer>> itemList = new ArrayList<>(itemCounts.entrySet());
            itemList.sort((a, b) -> {
                int cp_val = b.getValue().compareTo(a.getValue());
                return (cp_val != 0) ? cp_val : a.getKey().compareTo(b.getKey());
            });

            StringBuilder res = new StringBuilder();
            for (Map.Entry<String, Integer> one_item : itemList) {
                res.append("[").append(one_item.getKey()).append(", ").append(one_item.getValue()).append("] ");
            }

            context.write(key, new Text(res.toString().trim()));
        }
    }

    public static class TransactionItemCount extends Configured implements Tool {
        public int run(String[] args) throws Exception {
            Configuration conf = getConf();
            Job job = Job.getInstance(conf, "Transaction Item Count");

            job.setJarByClass(Ques_one.class);
            job.setMapperClass(Ques_oneMapper.class);
            job.setReducerClass(Ques_oneReducer.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));

            return job.waitForCompletion(true) ? 0 : 1;
        }

        public static void main(String[] args) throws Exception {
            int exitCode = ToolRunner.run(new TransactionItemCount(), args);
            System.exit(exitCode);
        }
    }
}
