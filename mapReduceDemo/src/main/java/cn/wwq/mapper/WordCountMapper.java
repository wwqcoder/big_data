package cn.wwq.mapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 *   k1:   行偏移量    long  LongWritable
 *   v1:   内容       String  Text
 *
 *   k2:   一个单词   String  Text
 *   v2:   固定值     long  LongWritable
 */
public class WordCountMapper extends Mapper<LongWritable, Text,Text,LongWritable> {

    /**
     *
     * @param key    k1
     * @param value   v1
     * @param context   上下文对象
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();
        String[] split = line.split(",");
        for (String word : split) {
            //                      k2           v2
            context.write(new Text(word),new LongWritable(1));
        }
    }
}
