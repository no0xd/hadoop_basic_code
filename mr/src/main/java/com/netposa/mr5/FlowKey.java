package com.netposa.mr5;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FlowKey implements WritableComparable<FlowKey> {

    private Long downStreamCount;
    private String phoneNum;

    public FlowKey() {

    }

    public FlowKey(Long downStreamCount, String phoneNum) {
        this.downStreamCount = downStreamCount;
        this.phoneNum = phoneNum;
    }

    public Long getDownStreamCount() {
        return downStreamCount;
    }

    public void setDownStreamCount(Long downStreamCount) {
        this.downStreamCount = downStreamCount;
    }

    public String getPhoneNum() {
        return phoneNum;
    }

    public void setPhoneNum(String phoneNum) {
        this.phoneNum = phoneNum;
    }

    @Override
    public int compareTo(FlowKey o) {
        int res=(int)(this.downStreamCount-o.getDownStreamCount()) * -1;

        if(res==0){
            return phoneNum.compareTo(o.getPhoneNum());
        }

        return res;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(downStreamCount);
        dataOutput.writeUTF(phoneNum);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.downStreamCount=dataInput.readLong();
        this.phoneNum=dataInput.readUTF();
    }
}
