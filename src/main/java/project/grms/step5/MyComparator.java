package project.grms.step5;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * @program: shuaixiaohuo.java
 * @Description: GRMS
 * @author: ZZZss
 * @Created 2019/10/14 19:10
 */
public class MyComparator  extends WritableComparator {


    public MyComparator() {
        super(superKey.class,true);

    }


    @Override
    public int compare(WritableComparable a, WritableComparable b) {

        superKey ska= (superKey) a;
        superKey skb= (superKey) b;

        return  super.compare(ska.getGid(),skb.getGid());


    }
}
