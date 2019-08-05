package com.netposa.mr;

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

public class JobMain extends Configured implements Tool {

    static Path inputPath;
    static Path outputPath;

    @Override
    public int run(String[] strings) throws Exception {
        //创建任务对象
        Job job = Job.getInstance(getConf(), "wc");
        //打包放在集群运行时,需要做一个配置
        job.setJarByClass(JobMain.class);

        //1.设置读取文件类及路径(由此确定k1,v1)
        job.setInputFormatClass(TextInputFormat.class);
        //TextInputFormat.addInputPath(job,new Path("hdfs://node0:9000/user/root/hello"));

        TextInputFormat.addInputPath(job, inputPath);
        //2.设置mapper类(k2,v2)
        job.setMapperClass(WCMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);


        //shuffle阶段
        //分区,排序,归约,分组 3,4,5,6步皆可省略

        //3.设置分区类(需要根据分区的个数调整reducer的个数)
        job.setPartitionerClass(WCPartitioner.class);


        //7.设置reducer(k3,v3)
        job.setNumReduceTasks(2);
        job.setReducerClass(WCReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        //8.设置输出类及路径
        job.setOutputFormatClass(TextOutputFormat.class);

        TextOutputFormat.setOutputPath(job, outputPath);

        //成功返回0
        return job.waitForCompletion(true)?0:1;
    }

    public static void main(String[] args) throws  Exception{

        for(int i=0;i<args.length;i++){
            System.out.println(i+"=>"+args[i]);
        }

        inputPath = new Path(args[0]);
        outputPath = new Path(args[1]);

        //启动一个任务
        Configuration conf = new Configuration();

        int run = ToolRunner.run(conf, new JobMain(), args);
        System.exit(run);

    }
}
