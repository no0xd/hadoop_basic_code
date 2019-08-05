package com.netposa.mr2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;

public class SortMapper extends Mapper<LongWritable,Text,SoftPairWritable,NullWritable>{

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = StringUtils.split(value.toString(), '\t');
        System.out.println("split[0]=>"+split[0]);
        System.out.println("split[1]=>"+split[1]);

        SoftPairWritable sp=null;
        if(split.length==2){
            sp=new SoftPairWritable(split[0],Long.parseLong(split[1]));
            context.write(sp,NullWritable.get());
        }
    }
}
