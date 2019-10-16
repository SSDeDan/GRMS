package project.grms.step5;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

/**
 * @program: shuaixiaohuo.java
 * @Description: GRMS
 * @author: ZZZss
 *
 *          自定义一个数据类型， 价格标记
 *
 * @Created 2019/10/14 11:13
 */
public class superKey implements WritableComparable<superKey>  {


    private Text gid;
    private Text flag;

    public superKey(){
        this.gid=new Text();
        this.flag=new Text();
    }

    @Override
    public int hashCode(){
        return Objects.hash(getGid(),getFlag());
    }

    @Override
    public boolean equals(Object o){
        if(this==o) return true;
        if(!(o instanceof superKey)) return false;
        superKey tupleKey=(superKey)o;
        return getGid().equals(tupleKey.getGid())&&getFlag().equals(tupleKey.getFlag());
    }

    @Override
    public String toString(){
        return this.gid+"\t"+this.flag;
    }

    public Text getGid(){
        return gid;
    }

    public void setGid(Text gid){
        this.gid.set(gid.toString());
    }

    public void setGid(String gid){
        this.gid.set(gid);
    }

    public Text getFlag(){
        return flag;
    }

    public void setFlag(Text flag){
        this.flag.set(flag.toString());
    }

    public void setFlag(String flag){
        this.flag.set(flag);
    }

    @Override
    public int compareTo(superKey o){
        int gidComp=this.gid.compareTo(o.gid);
        int flagComp=this.flag.compareTo(o.flag);
        return gidComp==0?flagComp:gidComp;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException{
        this.gid.write(dataOutput);
        this.flag.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException{
        this.gid.readFields(dataInput);
        this.flag.readFields(dataInput);
    }

}
