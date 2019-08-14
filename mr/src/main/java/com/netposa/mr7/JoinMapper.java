package com.netposa.mr7;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;

public class JoinMapper extends Mapper<Object,Text,Text,Text> {


    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        FileSplit fileSplit= (FileSplit)context.getInputSplit();
        String fileName = fileSplit.getPath().getName();


        String[] words = StringUtils.split(value.toString(), ',');
        if(fileName.equals("orders.txt")){
            context.write(new Text(words[2]),value);
        }else{
            context.write(new Text(words[0]),value);
        }
    }
}
