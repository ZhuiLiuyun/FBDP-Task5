package org.example.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import java.io.*;
import java.util.*;

public class Top100WordCount {

    // 内部类：保存单词及其计数
    static class WordCount {
        String word;
        int count;
        WordCount(String w, int c) {
            word = w;
            count = c;
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: Top100WordCount <input> <output>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);
        if (fs.exists(outputPath)) fs.delete(outputPath, true);
        fs.mkdirs(outputPath);

        List<WordCount> positiveList = new ArrayList<>();
        List<WordCount> negativeList = new ArrayList<>();

        // 遍历输入路径下的所有文件
        RemoteIterator<LocatedFileStatus> files = fs.listFiles(inputPath, true);
        while (files.hasNext()) {
            Path p = files.next().getPath();
            String fileName = p.getName().toLowerCase();

            // 只处理包含 positive 或 negative 的文件
            if (!fileName.contains("positive") && !fileName.contains("negative")) continue;

            System.out.println("Reading file: " + fileName);

            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(p)));
            String line;
            while ((line = br.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty()) continue;

                String[] parts = line.split("\\s+");
                if (parts.length < 2) continue;

                String word = parts[0];
                int count;
                try {
                    count = Integer.parseInt(parts[1]);
                } catch (NumberFormatException e) {
                    continue; // 跳过异常行
                }

                if (fileName.contains("positive")) {
                    positiveList.add(new WordCount(word, count));
                } else if (fileName.contains("negative")) {
                    negativeList.add(new WordCount(word, count));
                }
            }
            br.close();
        }

        // 排序（按出现次数降序）
        Comparator<WordCount> cmp = (a, b) -> Integer.compare(b.count, a.count);
        positiveList.sort(cmp);
        negativeList.sort(cmp);

        // 写出正面 top100
        BufferedWriter bwPos = new BufferedWriter(
                new OutputStreamWriter(fs.create(new Path(outputPath, "positive_top100.txt")))
        );
        for (int i = 0; i < Math.min(100, positiveList.size()); i++) {
            bwPos.write(positiveList.get(i).word + "\t" + positiveList.get(i).count + "\n");
        }
        bwPos.close();

        // 写出负面 top100
        BufferedWriter bwNeg = new BufferedWriter(
                new OutputStreamWriter(fs.create(new Path(outputPath, "negative_top100.txt")))
        );
        for (int i = 0; i < Math.min(100, negativeList.size()); i++) {
            bwNeg.write(negativeList.get(i).word + "\t" + negativeList.get(i).count + "\n");
        }
        bwNeg.close();

        System.out.println("✅ Top100WordCount finished successfully.");
    }
}
