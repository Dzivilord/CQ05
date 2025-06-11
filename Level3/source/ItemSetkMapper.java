import java.io.IOException;
import java.io.FileReader;
import java.io.BufferedReader;
import java.util.*;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

public class ItemSetkMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Set<String> frequentItems;
    private int k;

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
        k=context.getConfiguration().getInt("k", 2);
        frequentItems = new HashSet<>();

        String prevOutputPath = context.getConfiguration().get("prevOutputPath");
        Configuration conf = context.getConfiguration();
        FileSystem fs = FileSystem.get(conf);

        Path path = new Path(prevOutputPath + "/part-r-00000");
        if (!fs.exists(path)) {
            throw new IOException("Previous output file does not exist: " + path);
        }
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(path)))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split("\t");
                if (parts.length >= 1) {
                    frequentItems.add(parts[0]);
                }
            }
        }
    }

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString().trim();
        String[] parts = line.split("\t");
        if (parts.length < 2) {
            return;
        }

        String[] items = parts[1].split(" ");
        Arrays.sort(items);

        List<String> k_items = generatekItemsets(items,k);
        for (String k_item : k_items){
            if (isValidItem(k_item,frequentItems,k)){
                context.write(new Text(k_item),one);
            }
        }
    }

    public List<String> generatekItemsets(String[] items, int k){
        List<String> k_itemsets = new ArrayList<>();
        generateCombinations(items,0,k,new ArrayList<>(), k_itemsets);
        return k_itemsets;
    }

    public void generateCombinations(String[] items, int start, int k,List<String> current, List<String>k_items){
        if (current.size()==k){
            StringJoiner joiner=new StringJoiner(" ");
            for (String item : current) {
                joiner.add(item);
            }
            k_items.add(joiner.toString());
            return;
        }

        for (int i=start;i<items.length;i++){
            current.add(items[i]);
            generateCombinations(items, i+1, k, current, k_items);
            current.remove(current.size() - 1);
        }
    }

    public boolean isValidItem(String item, Set<String> frequentItems, int k) {
        if (k==1) return true;
        String[] items=item.split(" ");
        List<String> subsets= generatekItemsets(items, k-1);
        for (String subset : subsets) {
            if (!frequentItems.contains(subset)) {
                return false;
            }
        }
        return true;
    }
}