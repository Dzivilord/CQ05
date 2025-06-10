import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class GroupStatsJob {
    public static class GroupStatsMapper extends Mapper<Object, Text, Text, IntWritable> {
        private Text group = new Text();
        private IntWritable point = new IntWritable();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()) return;
            String[] parts = line.split("\\t");
            if (parts.length != 2) return;
            group.set(parts[0].trim());
            try {
                point.set(Integer.parseInt(parts[1].trim()));
                context.write(group, point);
            } catch (NumberFormatException e) {
                // Ignore lines with invalid number
            }
        }
    }

    public static class GroupStatsReducer extends Reducer<Text, IntWritable, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            ArrayList<Integer> pts = new ArrayList<>();
            double sum = 0;
            int count = 0;

            for (IntWritable val : values) {
                pts.add(val.get());
                sum += val.get();
                count++;
            }

            if (count == 0) return;

            Collections.sort(pts);
            double mean = sum / count;
            double median;
            if (count % 2 == 1) {
                median = pts.get(count / 2);
            } else {
                median = (pts.get(count / 2 - 1) + pts.get(count / 2)) / 2.0;
            }

            // Format median: nếu là số nguyên thì in không có phần thập phân
            String medianStr = (median % 1 == 0) ? String.valueOf((int)median) : String.format("%.2f", median);
            String meanStr = String.format("%.2f", mean);

            context.write(key, new Text(meanStr + "\t" + medianStr));
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: GroupStatsJob <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Group Stats");
        job.setJarByClass(GroupStatsJob.class);

        job.setMapperClass(GroupStatsMapper.class);
        job.setReducerClass(GroupStatsReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}