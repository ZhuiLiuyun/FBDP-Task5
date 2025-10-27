package org.example.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import java.net.URI;

public class StockSentimentDriver {
    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: StockSentimentDriver <input> <output> <stopwordfile>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Stock Sentiment Word Count");
        job.setJarByClass(StockSentimentDriver.class);

        job.setMapperClass(StockSentimentMapper.class);
        job.setReducerClass(StockSentimentReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.addCacheFile(new URI(args[2]));

        MultipleOutputs.addNamedOutput(job, "positive", 
                org.apache.hadoop.mapreduce.lib.output.TextOutputFormat.class, Text.class, IntWritable.class);
        MultipleOutputs.addNamedOutput(job, "negative", 
                org.apache.hadoop.mapreduce.lib.output.TextOutputFormat.class, Text.class, IntWritable.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
