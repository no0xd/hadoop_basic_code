package com.netposa.mr2;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class SoftPairWritable implements WritableComparable<SoftPairWritable>{
    private String first;
    private Long second;

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(first);
        dataOutput.writeLong(second);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.first=dataInput.readUTF();
        this.second=dataInput.readLong();
    }

    public SoftPairWritable() {
    }

    public SoftPairWritable(String first, Long second) {
        this.first = first;
        this.second = second;
    }

    public String getFirst() {
        return first;
    }

    public void setFirst(String first) {
        this.first = first;
    }

    public Long getSecond() {
        return second;
    }

    public void setSecond(Long second) {
        this.second = second;
    }



    @Override
    public int compareTo(SoftPairWritable o) {
        int result=this.first.compareTo(o.first);
        if(result==0){
            return (int)(this.second-o.second);
        }
        return result;
    }

    @Override
    public String toString() {
        return first+"\t"+second;
    }
}