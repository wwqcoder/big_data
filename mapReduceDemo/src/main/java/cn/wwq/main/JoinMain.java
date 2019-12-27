package cn.wwq.main;

import cn.wwq.combine.WordCountCombine;
import cn.wwq.mapper.JoinMapper;
import cn.wwq.mapper.WordCountMapper;
import cn.wwq.reduce.JoinReducer;
import cn.wwq.reduce.WordCountReduce;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class JoinMain extends Configured implements Tool {

    public int run(String[] strings) throws Exception {

        Job job = Job.getInstance(super.getConf(), "mapreduce-joinJob");
        //打包到集群上面运行时候，必须要添加以下配置，指定程序的main函数
        job.setJarByClass(JoinMain.class);
        //第一步:读取输入文件解析成key，value对
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job,new Path("hdfs://node01:8020/join"));
        //第二步:设置我们的mapper类
        job.setMapperClass(JoinMapper.class);
        //设置我们map阶段完成之后的输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        //第三步，第四步，第五步，第六步，省略

        // 第七步:设置我们的reduce类
        job.setReducerClass(JoinReducer.class);
        //设置我们reduce阶段完成之后的输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        //第八步:设置输出类以及输出路径
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job,new Path("hdfs://node01:8020/join_out"));
        boolean b = job.waitForCompletion(true);
        return b?0:1;
    }

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        int run = ToolRunner.run(configuration, new JoinMain(), args);
        System.exit(run);
    }
}
