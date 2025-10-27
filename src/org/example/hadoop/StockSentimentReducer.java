package org.example.hadoop;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import java.io.IOException;

public class StockSentimentReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private MultipleOutputs<Text, IntWritable> mos;

    @Override
    protected void setup(Context context) {
        mos = new MultipleOutputs<>(context);
    }

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        String[] parts = key.toString().split("\t");
        if (parts.length != 2) return;

        String sentiment = parts[0];
        String word = parts[1];
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }

        if (sentiment.equals("1")) {
            mos.write("positive", new Text(word), new IntWritable(sum));
        } else if (sentiment.equals("-1")) {
            mos.write("negative", new Text(word), new IntWritable(sum));
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        mos.close();
    }
}
