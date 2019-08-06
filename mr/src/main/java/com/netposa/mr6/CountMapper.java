package com.netposa.mr6;

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
        if(split.length<5){
            System.out.println("校验未通过,当前行舍弃 =>"+line);
            return;
        }

        phoneNumber=split[0];
        upPackageCount=Long.parseLong(split[1]);
        downPackageCount=Long.parseLong(split[2]);
        upStreamCount=Long.parseLong(split[3]);
        downStreamCount=Long.parseLong(split[4]);

        context.write(new Text(phoneNumber), new FlowCount(upPackageCount, downPackageCount, upStreamCount, downStreamCount));
    }
}
