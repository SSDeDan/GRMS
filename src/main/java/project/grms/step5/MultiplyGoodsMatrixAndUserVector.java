package project.grms.step5;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import project.grms.step4.UserBuyGoodsVector;

import java.io.IOException;
import java.util.HashMap;

/**
 * @program: shuaixiaohuo.java
 * @Description: GRMS
 * @author: ZZZss
 *
 *          方法一：
 *          采用了map集合的方法，将两个文件分别放入两个集合内
 *
 * @Created 2019/10/11 10:07
 */
public class MultiplyGoodsMatrixAndUserVector  extends Configured implements Tool {



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

//    in2   /grms/goods4
//      用户的购买向量
//                   20001	 10001:1,10004:1,10005:1
//                    20002	 10001:1,10003:1,10004:1
//                    20003	 10002:1
//                    20004	 10002:1,10006:1
//                    20005	 10001:1,10004:1
//                    20006	 10001:1,10002:1,10004:1
//                    20007	 10001:1,10003:1,10006:1


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



    static class MultiplyGoodsMatrixAndUserVectorFirstMapper
            extends Mapper<LongWritable, Text,Text,Text> {

        private Text k2=new Text();
        private Text v2=new Text();

        @Override
        protected void map(LongWritable k1, Text v1, Context context) throws IOException, InterruptedException {

            String[] strs = v1.toString().split("\t");
            this.k2.set(strs[0]);
            this.v2.set(strs[1]);

            System.out.println("a"+this.k2+"?????"+this.v2);
            context.write(this.k2,this.v2);

        }
    }


    static class MultiplyGoodsMatrixAndUserVectorSecondMapper
            extends Mapper<LongWritable, Text,Text,Text> {

        private Text k2=new Text();
        private Text v2=new Text();

        @Override
        protected void map(LongWritable k1, Text v1, Context context) throws IOException, InterruptedException {

            String[] strs = v1.toString().split("\t");
            this.k2.set(strs[0]);
            this.v2.set(strs[1]);
            System.out.println("b"+this.k2+"?????"+this.v2);
            context.write(this.k2,this.v2);


        }
    }


//        20001	 [10001:1,10004:1,10005:1     ,     20001:3,20002:2,20005:2,20006:2,20007:1]



    static class  MultiplyGoodsMatrixAndUserVectorReducer
            extends Reducer<Text,Text,Text, NullWritable> {

        private Text k3=new Text();
        private NullWritable v3=NullWritable.get();


        @Override
        protected void reduce(Text k2, Iterable<Text> v2s, Context context) throws IOException, InterruptedException {



            HashMap<String,Integer> map1=new HashMap<>();
            HashMap<String,Integer> map2=new HashMap<>();

            map1.clear();
            map2.clear();



            for (Text v2 : v2s) {


                if ("1".equals(v2.toString().substring(0,1))){

                     //   10001:1,10004:1,10005:1 将这个数据放入map1
                    String[] strs1 = v2.toString().split("[,]");

                    for (String s : strs1) {
                        String[] strs2 = s.split("[:]");
                        map1.put(strs2[0], Integer.valueOf(strs2[1]));
                    }


                }
                else  if ("2".equals(v2.toString().substring(0,1))){

                    //  20001:3,20002:2,20005:2,20006:2,20007:1
                    //将这个数据放入map2

                    String[] strs3 = v2.toString().split("[,]");

                    for (String s : strs3) {

                        String[] strs4 = s.split("[:]");

                        System.out.println(strs4[0]+"!!!!"+strs4[1]);
                        map2.put(strs4[0], Integer.valueOf(strs4[1]));
                    }

                }

            }



            map1.forEach((k,v)->{
                     //   10001:1,10004:1,10005:1

                map2.forEach((k1,v1)->{
                    //  20001:3,20002:2,20005:2,20006:2,20007:1

                    System.out.println(k1+"~~~~~"+v1);

                    this.k3.set(k+","+k1+"\t"+v*v1);

                    try {
                        context.write(this.k3,this.v3);
                    } catch (IOException e) {
                        e.printStackTrace();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }


                });




            });












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


        //map1端配置
        job.setMapperClass(MultiplyGoodsMatrixAndUserVectorFirstMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job,in1);

        //map2端配置
        job.setMapperClass(MultiplyGoodsMatrixAndUserVectorSecondMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job,in2);


        //reduce端配置
        job.setReducerClass(MultiplyGoodsMatrixAndUserVectorReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job,out);




        return job.waitForCompletion(true)?0:1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new MultiplyGoodsMatrixAndUserVector(),args));
    }
}
