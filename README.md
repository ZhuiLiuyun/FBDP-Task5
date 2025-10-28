# 作业5
统计正面/负面新闻标题中各自出现的前 100 个高频单词（忽略大小写、标点、数字、停词）

## 输入
- HDFS: `/input/stock_data.csv`
  - 每行格式：`<标题>,<情感标签>`（情感标签为 `1` 或 `-1`）
- HDFS: `/input/stop-word-list.txt`（每行一个停词）

## 输出
- HDFS: `/output/job1` —— 每行：`<label>\t<word>\t<count>` （这是第一步词频统计）
- HDFS: `/output/top100` —— 两个文件：
  - `positive_top100.txt`：前 100 个正面高频词（每行：`<word>\t<count>`）
  - `negative_top100.txt`：前 100 个负面高频词（每行：`<word>\t<count>`）

---

## 伪代码说明（总体流程）
### Mapper：StockSentimentMapper
**功能** 
逐行读取输入 CSV，清洗文本，分词，去停词后输出 (label, word) → 1。

**伪代码**
```
map(key, value):
    line = value.trim()
    if line == "":
        return

    lastComma = line.lastIndexOf(',')
    if lastComma < 0:
        return

    title = line.substring(0, lastComma)
    label = line.substring(lastComma + 1).trim()

    if label != "1" and label != "-1":
        return

    # 文本预处理
    title = title.toLowerCase()
    title = title.replaceAll("[^a-z]", " ")

    # 分词与过滤
    for token in title.split(whitespace):
        word = token.trim()
        if word == "" or word.length() <= 2:
            continue
        if word in stopWords:
            continue
        emit(label + "\t" + word, 1)
```

### Reducer（StockSentimentReducer）
**功能** 
对 Mapper 输出的相同 (label + word) 键进行累加求和，得到每个单词在对应情感下的总频次。

**伪代码**
```
reduce(key, values):
    total = 0
    for count in values:
        total += count

    emit(key, total)
```
### Driver（StockSentimentDriver）
**功能** 
配置并提交 MapReduce 作业，指定输入输出路径、Mapper 和 Reducer 类等。

**伪代码**
```
main(args):
    if len(args) != 2:
        print("Usage: StockSentimentDriver <input> <output>")
        exit(-1)

    conf = new Configuration()
    job = Job.getInstance(conf, "Stock Word Count")

    job.setMapperClass(StockSentimentMapper)
    job.setReducerClass(StockSentimentReducer)
    job.setOutputKeyClass(Text)
    job.setOutputValueClass(IntWritable)

    FileInputFormat.addInputPath(job, args[0])
    FileOutputFormat.setOutputPath(job, args[1])

    job.waitForCompletion(true)

```
### Top100 词频统计（Top100WordCount）
**功能** 
从 Reducer 输出的词频文件中读取数据，分别选出正向和负向的前 100 个高频词。

**伪代码**
```
main(args):
    if len(args) != 2:
        print("Usage: Top100WordCount <input> <output>")
        exit(-1)

    conf = new Configuration()
    fs = FileSystem.get(conf)

    inputPath = args[0]
    outputPath = args[1]
    if fs.exists(outputPath):
        fs.delete(outputPath, true)
    fs.mkdirs(outputPath)

    posMap = empty TreeMap()
    negMap = empty TreeMap()

    for each file in inputPath:
        if file name contains "positive" or "negative":
            for each line in file:
                (word, count) = parse(line)
                if file name contains "positive":
                    posMap.put(count, word)
                    if posMap.size() > 100:
                        posMap.remove(posMap.firstKey())
                else:
                    negMap.put(count, word)
                    if negMap.size() > 100:
                        negMap.remove(negMap.firstKey())

    write posMap (descending) to "positive_top100.txt"
    write negMap (descending) to "negative_top100.txt"
```
## 运行结果示例

<img width="2479" height="1424" alt="屏幕截图 2025-10-27 222031" src="https://github.com/user-attachments/assets/f729d10c-9b37-4a87-ae4b-2ae3635cc99c" />


## 主要问题
- 第一次运行时发现结果很少（negative里甚至不足100个），经过分析，是在统计时将出现次数相同的词只输出了一个，调整后解决
- 发现结果中含有1个字母的单词，通过加入筛选条件把这些单个单词过滤掉
