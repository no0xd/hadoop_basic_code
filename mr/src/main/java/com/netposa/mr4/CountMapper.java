package com.netposa.mr4;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;

public class CountMapper extends Mapper<Object,Text,Text,FlowCount> {

    private String phoneNumber;
    private Long upPackageCount=0L;
    private Long downPackageCount=0L;
    private Long upStreamCount=0L;
    private Long downStreamCount=0L;

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {


        String line = value.toString();
        String[] split = StringUtils.split(line,'\t');

        //用小于判断,便于后期拓展
        if(split.length<11){
            System.out.println("校验未通过,当前行舍弃 =>"+line);
            return;
        }

        phoneNumber=split[1];
        upPackageCount=Long.parseLong(split[6]);
        downPackageCount=Long.parseLong(split[7]);
        upStreamCount=Long.parseLong(split[8]);
        downStreamCount=Long.parseLong(split[9]);

        context.write(new Text(phoneNumber), new FlowCount(upPackageCount, downPackageCount, upStreamCount, downStreamCount));
    }
}
