package com.netposa.mr2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class JobMain2 extends Configured implements Tool{

    static Path inputPath;
    static Path outputPath;

    @Override
    public int run(String[] strings) throws Exception {

        Job job = Job.getInstance(getConf(),"sort");
        job.setJarByClass(JobMain2.class);

        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job,inputPath);
        job.setMapperClass(SortMapper.class);
        job.setMapOutputKeyClass(SoftPairWritable.class);
        job.setMapOutputValueClass(NullWritable.class);

        //分组、排序、归约、分区，框架自动执行


        job.setReducerClass(SortReducer.class);
        job.setOutputKeyClass(SoftPairWritable.class);
        job.setOutputValueClass(NullWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job,outputPath);

        return job.waitForCompletion(true)?0:1;

    }

    public static void main(String[] args) throws Exception{
        if(args.length<2){
            System.out.println("参数不能少于2个");
            System.out.println("inputPath -> "+args[0]);
            System.out.println("outputPath -> "+args[1]);
            System.exit(1);
        }else {
            inputPath = new Path(args[0]);
            outputPath = new Path(args[1]);
        }

        //启动一个任务
        int run = ToolRunner.run(new Configuration(), new JobMain2(), args);
        System.exit(run);
    }
}
