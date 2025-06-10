import java.io.IOException;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Ques_two {

    public static class Ques_twoMapper extends Mapper<LongWritable, Text, Text, Text>{
        private Text ID = new Text();
        private Text value = new Text();
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
            String[] parts_of_input = value.toString().split("\t");
            if (parts_of_input.length == 2){
                ID.set(parts_of_input[0].trim());
                value.set(parts_of_input[1].trim());
                context.write(ID, value);
            }
        }
    }
    public static class Ques_twoProducer extends Reducer<Text, Text, Text, DoubleWritable>{
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
            double cost = 0;
            for (Text val: values){
                String item = val.toString();
                String itemWithoutSpace = item.replaceAll(" ","");
                int len = itemWithoutSpace.length();
                if (item.contains("*")) {
                    cost = cost + (len - 1) * 0.8;
                } else {
                    cost = cost + (double)len;
                }
            }
            context.write(key, new DoubleWritable(cost));
        }
    }
    public static class TransactionItemCount extends Configured implements Tool {
        public int run(String[] args) throws Exception {
            Configuration conf = getConf();
            Job job = Job.getInstance(conf, "Transaction Item Cost Calculator");

            job.setJarByClass(Ques_two.class);
            job.setMapperClass(Ques_twoMapper.class);
            job.setReducerClass(Ques_twoProducer.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(DoubleWritable.class);

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