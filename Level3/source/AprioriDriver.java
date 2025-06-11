import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.*;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class AprioriDriver{
    public static void main(String[] args) throws Exception{
        if (args.length!=3){
            System.err.println("Usage: AprioriDriver <input path> <output path> <min support>");
            System.exit(-1);
        }

        String inputPath=args[0];
        String finalOutputPath=args[1];
        String tempOutputPath="/temp";

        double minSupport=Double.parseDouble(args[2]);

        System.out.println("Input Path: " + inputPath);
        System.out.println("Final Output Path: " + finalOutputPath);
        System.out.println("Minimum Support: " + minSupport);

        Configuration conf = new Configuration();
        conf.setDouble("minSupport", minSupport);
        FileSystem fs = FileSystem.get(conf);

        fs.delete(new Path(finalOutputPath), true);

        // Công việc 0: Đếm tổng số giao dịch
        Job job0 = Job.getInstance(conf, "Apriori Count Transactions");
        job0.setJarByClass(AprioriDriver.class);
        job0.setMapperClass(CountMapper.class);
        job0.setCombinerClass(CountCombiner.class);
        job0.setReducerClass(CountReducer.class);
        job0.setNumReduceTasks(1);
        job0.setInputFormatClass(TextInputFormat.class);
        job0.setOutputFormatClass(TextOutputFormat.class);
        job0.setOutputKeyClass(Text.class);
        job0.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job0, new Path(inputPath));
        FileOutputFormat.setOutputPath(job0, new Path(tempOutputPath + "/total"));
        boolean job0Completed = job0.waitForCompletion(true);
        if (!job0Completed) {
            System.err.println("Job 0 failed.");
            System.exit(1);
        }

        int totalTransactions = 0;
        Path totalOutputPath = new Path(tempOutputPath + "/total/part-r-00000");
        if (fs.exists(totalOutputPath)) {
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(totalOutputPath)))) {
                String line = reader.readLine();
                if (line != null) {
                    String[] parts = line.split("\t");
                    if (parts.length == 2 && parts[0].equals("TOTAL")) {
                        totalTransactions = Integer.parseInt(parts[1]);
                    }
                }
            }
        } else {
            System.err.println("Job 0 output file does not exist: " + totalOutputPath);
            System.exit(1);
        }

        System.out.println("Total transactions retrieved: " + totalTransactions); // Debug log
        if (totalTransactions <= 0) {
            System.err.println("Total transactions must be greater than zero.");
            System.exit(1);
        }

        conf.setInt("totalTransactions", totalTransactions);

        // Công việc 1: Tìm 1-item thường xuyên và tổng số giao dịch
        Job job1 = Job.getInstance(conf,"Apriori 1-itemset");
        job1.setJarByClass(AprioriDriver.class);
        job1.setMapperClass(ItemSet1Mapper.class);
        job1.setCombinerClass(ItemSet1Combiner.class);
        job1.setReducerClass(ItemSet1Reducer.class);

        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputFormatClass(TextOutputFormat.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job1, new Path(inputPath));
        FileOutputFormat.setOutputPath(job1, new Path(tempOutputPath + "/itemset1"));

        job1.waitForCompletion(true);

        // Kiểm tra đầu ra công việc 1
        Path job1Output = new Path(tempOutputPath + "/itemset1/part-r-00000");
        if (!fs.exists(job1Output)) {
            System.err.println("Job 1 output file does not exist: " + job1Output);
            System.exit(1);
        }


        // Công việc 2: Tìm k-itemset thường xuyên
        int k = 2;
        boolean hasFrequentItemsets = true;
        while(hasFrequentItemsets){
            int temp=k-1;
            conf.set("prevOutputPath", tempOutputPath + "/itemset" + String.valueOf(temp));
            conf.setInt("k", k);
            Job jobK= Job.getInstance(conf, "Apriori " + k + "-itemset");

            jobK.setJarByClass(AprioriDriver.class);
            jobK.setMapperClass(ItemSetkMapper.class);
            jobK.setCombinerClass(ItemSetkCombiner.class);
            jobK.setReducerClass(ItemSetkReducer.class);

            jobK.setInputFormatClass(TextInputFormat.class);
            jobK.setOutputFormatClass(TextOutputFormat.class);

            jobK.setOutputKeyClass(Text.class);
            jobK.setOutputValueClass(IntWritable.class);

            FileInputFormat.addInputPath(jobK, new Path(inputPath));

            String tempKOutputPath= tempOutputPath + "/itemset" + String.valueOf(k);
            FileOutputFormat.setOutputPath(jobK, new Path(tempKOutputPath));
            
            Path tempOutPath=new Path(tempKOutputPath + "/part-r-00000");

            boolean completed = jobK.waitForCompletion(true);
            boolean hasOutput = fs.exists(tempOutPath) && fs.getFileStatus(tempOutPath).getLen() > 0;

            hasFrequentItemsets = completed && hasOutput;
            k++;
        }

        // Merge
        Job mergeJob = Job.getInstance(conf, "Apriori Merge");
        mergeJob.setJarByClass(AprioriDriver.class);
        mergeJob.setMapperClass(MergeMapper.class);
        mergeJob.setReducerClass(MergeReducer.class);
        mergeJob.setInputFormatClass(TextInputFormat.class);
        mergeJob.setOutputFormatClass(TextOutputFormat.class);
        mergeJob.setOutputKeyClass(Text.class);
        mergeJob.setOutputValueClass(IntWritable.class);
        mergeJob.setNumReduceTasks(1);
        FileInputFormat.addInputPath(mergeJob, new Path(tempOutputPath + "/itemset*"));
        FileOutputFormat.setOutputPath(mergeJob, new Path(finalOutputPath));
        mergeJob.waitForCompletion(true);

        fs.delete(new Path(tempOutputPath), true); // Xóa thư mục tạm thời
    }
}