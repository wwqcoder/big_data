package cn.wwq.partitioner;

import cn.wwq.entity.FlowBean;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class FlowPartition extends Partitioner<FlowBean,Text> {

    @Override
    public int getPartition(FlowBean flowBean, Text text, int i) {
        String line = text.toString();
        if (line.startsWith("135")){
            return 0;
        }else if (line.startsWith("136")){
            return 1;
        }else if (line.startsWith("137")){
            return 2;
        }
        return 3;    }
}
