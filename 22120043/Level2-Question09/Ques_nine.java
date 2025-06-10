import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Ques_nine extends Configured implements Tool {

    public static class Ques_nineMapper extends Mapper<LongWritable, Text, Text, Text> {
        private Text ID = new Text();
        private Text item = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts_of_input = value.toString().split("\\s+");
            if (parts_of_input.length == 3) {
                String food = parts_of_input[1].trim();
                String table = parts_of_input[0].trim();
                String price = parts_of_input[2].trim();
                ID.set(food);
                item.set(table + ':' + price);
                context.write(ID, item);
            }
        }
    }

    public static class Ques_nineProducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int price_quan = -1;
            int price_rice = -1;
            for (Text val : values) {
                String val_String = val.toString();
                String[] parts_of_String = val_String.split(":");
                if (parts_of_String[0].equals("FoodQuantity")) {
                    price_quan = Integer.parseInt(parts_of_String[1]);
                } else if (parts_of_String[0].equals("FoodPrice")) {
                    price_rice = Integer.parseInt(parts_of_String[1]);
                }
            }
            if (price_rice != -1) {
                if (price_quan != -1) {
                    String res = price_rice + " " + price_quan;
                    context.write(new Text(key.toString() + " " + res.toString()), new Text(""));

                } else {
                    String res =price_rice + " null";
                    res = res.trim();
                    context.write(new Text(key.toString() + " " + res.toString()), new Text(""));

                }
            }
        }
    }

    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        Job job = Job.getInstance(conf, "Left Outer Join");

        job.setJarByClass(Ques_nine.class);
        job.setMapperClass(Ques_nineMapper.class);
        job.setReducerClass(Ques_nineProducer.class); // FIXED

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Ques_nine(), args);
        System.exit(exitCode);
    }
}
