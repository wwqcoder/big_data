package cn.wwq.main;

import cn.wwq.entity.PairWritable;
import cn.wwq.mapper.SortMapper;
import cn.wwq.mapper.WordCountMapper;
import cn.wwq.partitioner.WordCountPartitioner;
import cn.wwq.reduce.SortReduce;
import cn.wwq.reduce.WordCountReduce;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class SortJobMain extends Configured implements Tool {

    public int run(String[] strings) throws Exception {

        Job job = Job.getInstance(super.getConf(), "mapreduce-sortJob");
        //打包到集群上面运行时候，必须要添加以下配置，指定程序的main函数
        job.setJarByClass(SortJobMain.class);
        //第一步:读取输入文件解析成key，value对
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job,new Path("hdfs://node01:8020/sort"));
        //第二步:设置我们的mapper类
        job.setMapperClass(SortMapper.class);
        //设置我们map阶段完成之后的输出类型
        job.setMapOutputKeyClass(PairWritable.class);
        job.setMapOutputValueClass(Text.class);

        //第三步，第四步，第五步，第六步，省略

        /**
         * 设置我们的分区类，以及我们的reducetask的个数，注意reduceTask的个数
         一定要与我们的
         * 分区数保持一致
         */



        // 第七步:设置我们的reduce类
        job.setReducerClass(SortReduce.class);
        //设置我们reduce阶段完成之后的输出类型
        job.setOutputKeyClass(PairWritable.class);
        job.setOutputValueClass(NullWritable.class);

        //第八步:设置输出类以及输出路径
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job,new Path("hdfs://node01:8020/sort_out"));
        boolean b = job.waitForCompletion(true);
        return b?0:1;
    }

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        int run = ToolRunner.run(configuration, new SortJobMain(), args);
        System.exit(run);
    }
}
