package project.grms.step5;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;

/**
 * @program: shuaixiaohuo.java
 * @Description: GRMS
 * @author: ZZZss
 * @Created 2019/10/14 18:59
 */
public class MultiplyGoodsMatrixAndUserVector1  extends Configured implements Tool {





//    in1   /grms/goods3
//      物品的共现矩阵
//                    20001	20001:3,20002:2,20005:2,20006:2,20007:1
//                    20002	20001:2,20002:3,20005:2,20006:2,20007:2
//                    20003	20003:1,20004:1,20006:1
//                    20004	20003:1,20004:2,20006:1,20007:1
//                    20005	20001:2,20002:2,20005:2,20006:2,20007:1
//                    20006	20001:2,20002:2,20003:1,20004:1,20005:2,20006:3,20007:1
//                    20007	20001:1,20002:2,20004:1,20005:1,20006:1,20007:3
//
//

  public   static class MultiplyGoodsMatrixAndUserVector1FirstMapper
            extends Mapper<Text, Text,superKey,Text> {

        private  superKey k2=new superKey();
        private  Text v2=new Text();


        @Override
        protected void map(Text k1, Text v1, Context context) throws IOException, InterruptedException {


            this.k2.setGid(k1.toString());
            this.k2.setFlag("a");
            this.v2.set(v1.toString());
            context.write(this.k2,this.v2);



        }
    }



    //    in2   /grms/goods4
//      用户的购买向量
//                   20001	 10001:1,10004:1,10005:1
//                    20002	 10001:1,10003:1,10004:1
//                    20003	 10002:1
//                    20004	 10002:1,10006:1
//                    20005	 10001:1,10004:1
//                    20006	 10001:1,10002:1,10004:1
//                    20007	 10001:1,10003:1,10006:1




   public static class MultiplyGoodsMatrixAndUserVector1SecondMapper
            extends Mapper<Text, Text,superKey,Text> {

        private  superKey k2=new superKey();
        private  Text v2=new Text();


        @Override
        protected void map(Text k1, Text v1, Context context) throws IOException, InterruptedException {

            this.k2.setGid(k1.toString());
            this.k2.setFlag("b");
            this.v2.set(v1.toString());
            context.write(this.k2,this.v2);

        }
    }






    //输出结果

//             10001,20001	2
//            10001,20001	2
//            10001,20001	3
//            10001,20001	1
//            10001,20001	2
//            10001,20002	3
//            10001,20002	2
//            10001,20002	2
//            10001,20002	2
//            10001,20002	2
//            10001,20003	1
//            10001,20004	1
//            10001,20004	1
//            10001,20005	2
//            10001,20005	2
//            10001,20005	2







    //        20001	 [     20001:3,20002:2,20005:2,20006:2,20007:1    ,    10001:1,10004:1,10005:1     ]


 public    static class  MultiplyGoodsMatrixAndUserVector1Reducer
            extends Reducer<superKey,Text,Text, IntWritable> {

         private  Text k3=new Text();
        private IntWritable v3=new IntWritable();

        @Override
        protected void reduce(superKey k2, Iterable<Text> v2s, Context context) throws IOException, InterruptedException {

            Iterator<Text> it = v2s.iterator();

//            20001:3,20002:2,20005:2,20006:2,20007:1
            String gm = it.next().toString();
//            10001:1,10004:1,10005:1
            String um = it.next().toString();

            String[] gms = gm.split("[,]");
            String[] ums = um.split("[,]");

            for (String gs : gms) {
                String[] gsm = gs.split("[:]");
                for (String us : ums) {
                    String[] usm = us.split("[:]");

                    this.k3.set(usm[0]+","+gsm[0]);
                    this.v3.set(Integer.parseInt(gsm[1])*Integer.parseInt(usm[1]));
                    context.write(this.k3,this.v3);


                }


            }


        }

    }

    @Override
    public int run(String[] strings) throws Exception {

        Configuration conf = this.getConf();
        Path in1 = new Path(conf.get("in1"));
        Path in2 = new Path(conf.get("in2"));
        Path out = new Path(conf.get("out"));


        Job job = Job.getInstance(conf,"商品共现矩阵乘以用户购买向量，形成临时的推荐结果");
        job.setJarByClass(this.getClass());



        MultipleInputs.addInputPath(job,in1, KeyValueTextInputFormat.class, MultiplyGoodsMatrixAndUserVector1FirstMapper.class);
        MultipleInputs.addInputPath(job,in2,KeyValueTextInputFormat.class, MultiplyGoodsMatrixAndUserVector1SecondMapper.class);

        job.setMapOutputKeyClass(superKey.class);
        job.setMapOutputValueClass(Text.class);


        //reduce端配置
        job.setReducerClass(MultiplyGoodsMatrixAndUserVector1Reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job,out);

        //设置分区器
        job.setPartitionerClass(Mypartitioner.class);

        //设置分组器
        job.setGroupingComparatorClass(MyComparator.class);


        return job.waitForCompletion(true)?0:1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new MultiplyGoodsMatrixAndUserVector1(),args));
    }
}
