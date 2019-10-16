package project.grms.step5;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @program: shuaixiaohuo.java
 * @Description: GRMS
 * @author: ZZZss
 *
 *      自定义一个分区器
 *
 * @Created 2019/10/14 19:05
 */
public class Mypartitioner  extends Partitioner<superKey, Text> {

    @Override
    public int getPartition(superKey superKey, Text text, int i) {
        return superKey.getGid().hashCode()%i ;
    }
}
