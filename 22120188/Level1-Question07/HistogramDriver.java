import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class HistogramDriver {
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: HistogramDriver <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Pixel Histogram");
        job.setJarByClass(HistogramDriver.class);

        job.setMapperClass(HistogramMapper.class);
        job.setCombinerClass(HistogramReducer.class);  // Combiner để giảm data transfer
        job.setReducerClass(HistogramReducer.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));   // thư mục hoặc file đầu vào
        FileOutputFormat.setOutputPath(job, new Path(args[1])); // thư mục đầu ra

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
