package org.example.hadoop;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import java.io.*;
import java.net.URI;
import java.util.*;

public class StockSentimentMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private Set<String> stopWords = new HashSet<>();

    @Override
    protected void setup(Context context) throws IOException {
        URI[] cacheFiles = context.getCacheFiles();
        if (cacheFiles != null && cacheFiles.length > 0) {
            FileSystem fs = FileSystem.get(context.getConfiguration());
            BufferedReader reader = new BufferedReader(
                    new InputStreamReader(fs.open(new Path(cacheFiles[0]))));
            String line;
            while ((line = reader.readLine()) != null) {
                stopWords.add(line.trim().toLowerCase());
            }
            reader.close();
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString().trim();
        if (line.isEmpty()) return;

        // 找到最后一个逗号，左边是标题，右边是情感标签
        int lastComma = line.lastIndexOf(",");
        if (lastComma < 0) return;

        String title = line.substring(0, lastComma).replaceAll("\"", "");
        String sentiment = line.substring(lastComma + 1).trim();

        if (!sentiment.equals("1") && !sentiment.equals("-1")) return;

        // 清洗文本
        title = title.replaceAll("[^a-zA-Z]", " ").toLowerCase();

        for (String word : title.split("\\s+")) {
            //if (word.isEmpty() || stopWords.contains(word)) continue;
            if (!stopWords.contains(word) && word.length() >= 2)
            context.write(new Text(sentiment + "\t" + word), new IntWritable(1));
        }
    }
}
