package cn.wwq.partitioner;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class WordCountPartitioner extends Partitioner<Text, LongWritable> {

    /**
     *
     * @param text         k1   单词名字
     * @param longWritable  v1   固定值1
     * @param i
     * @return
     */
    public int getPartition(Text text, LongWritable longWritable, int i) {

        if (text.toString().length() > 5){
            return 0;
        }else {
            return 1;
        }
    }
}
