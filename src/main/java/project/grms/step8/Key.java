package project.grms.step8;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Objects;

/**
 * @program: shuaixiaohuo.java
 * @Description: GRMS
 * @author: ZZZss
 * @Created 2019/10/15 9:50
 */
public class Key implements  WritableComparable<Key>,  DBWritable {

    private Text uid;
    private Text gid;
    private IntWritable exp;


    public Key() {
        this.uid=new Text();
        this.gid=new Text();
        this.exp=new IntWritable();
    }



    //数据库序列化和反序列化
    @Override
    public void write(PreparedStatement ps) throws SQLException {
        //insert into grms.results(uid,gid,exp)
        //value(?,?,?);

        ps.setString(1,this.uid.toString());
        ps.setString(2,this.gid.toString());
        ps.setInt(3,this.exp.get());

    }

    @Override
    public void readFields(ResultSet rs) throws SQLException {
        //select uid,gid,exp from grms.results;

        this.uid.set(rs.getString(1));
        this.gid.set(rs.getString(2));
        this.exp.set(rs.getInt(3));


    }


    //HDFS的序列化和反序列化
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        this.uid.write(dataOutput);
        this.gid.write(dataOutput);
        this.exp.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.uid.readFields(dataInput);
        this.gid.readFields(dataInput);
        this.exp.readFields(dataInput);

    }



    public Text getUid() {
        return uid;
    }

    public void setUid(Text uid) {
        this.uid.set(uid.toString());
    }
    public void setUid(String uid) {
        this.uid.set(uid);
    }



    public Text getGid() {
        return gid;
    }

    public void setGid(Text gid) {
        this.gid.set(gid.toString());
    }

    public void setGid(String gid) {
        this.gid.set(gid);
    }



    public IntWritable getExp() {
        return exp;
    }

    public void setExp(IntWritable exp) {
        this.exp.set(exp.get());
    }
    public void setExp(int exp) {
        this.exp.set(exp);
    }


    @Override
    public int compareTo(Key o) {

        int uc = this.uid.compareTo(o.uid);
        int gc = this.gid.compareTo(o.gid);
        int ec = this.exp.compareTo(o.exp);

        return uc==0?(gc==0?ec:gc):uc;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Key key = (Key) o;
        return Objects.equals(uid, key.uid) &&
                Objects.equals(gid, key.gid) &&
                Objects.equals(exp, key.exp);
    }

    @Override
    public int hashCode() {
        return Objects.hash(uid, gid, exp);
    }

    @Override
    public String toString() {
        return "Key{" +
                "uid=" + uid +
                ", gid=" + gid +
                ", exp=" + exp +
                '}';
    }
}
