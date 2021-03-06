package cn.wwq.reduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 *   k2: 一个单词
 *   v2：值集合
 *
 *   k3:和k2一样
 *   v3：集合总数
 *
 */
public class WordCountReduce extends Reducer<Text, LongWritable,Text,LongWritable> {

    public static enum  Counter{
        REDUCE_INPUT_RECORDS, REDUCE_INPUT_VAL_NUMS,
    }

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        context.getCounter(Counter.REDUCE_INPUT_RECORDS).increment(1L);
        long count = 0;
        for (LongWritable value : values) {
            count += value.get();
            context.getCounter(Counter.REDUCE_INPUT_VAL_NUMS).increment(1L);

        }
        context.write(key,new LongWritable(count));
    }
}
