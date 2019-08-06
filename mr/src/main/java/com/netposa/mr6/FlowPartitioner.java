package com.netposa.mr6;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class FlowPartitioner extends Partitioner<Text,FlowCount>{
    public FlowPartitioner() {
        super();
    }

    /**
     *
     * @param text
     * @param flowCount
     * @param i reducer的个数
     * @return
     */
    @Override
    public int getPartition(Text text, FlowCount flowCount, int i) {
        String phoneNum = text.toString();
        if(phoneNum.startsWith("136")){
            return 0;
        }else if(phoneNum.startsWith("137")){
            return 1;
        }else{
            return 2;
        }
    }
}
