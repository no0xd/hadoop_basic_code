package com.netposa.mr7;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class JoinReducer extends Reducer<Text, Text, Text, Text> {


    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String firstProduct="";
        String secondOrders="";
        for(Text text:values){
            // 商品p打头
            if(text.toString().startsWith("p")){
                firstProduct=text.toString();
            }else {
                secondOrders=text.toString();
            }
        }

        if(firstProduct.equals("")){
            firstProduct="NULL";
        }

        context.write(key,new Text(firstProduct+"\t"+secondOrders));

    }
}
