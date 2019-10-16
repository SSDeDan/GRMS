package project.grms.step7;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import project.grms.step5.MultiplyGoodsMatrixAndUserVector;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;

/**
 * @program: shuaixiaohuo.java
 * @Description: GRMS
 * @author: ZZZss
 * @Created 2019/10/11 20:15
 */
public class DuplicateDataForResult extends Configured implements Tool {


//    1.FirstMapper处理用户的购买列表数据。
//    /grms/goods1

//             10001	20001,20005,20006,20007,20002
//             10002	20006,20003,20004
//             10003	20002,20007
//             10004	20001,20002,20005,20006
//             10005	20001
//             10006	20004,20007


//    2.SecondMapper处理第6的推荐结果数据。

    //    /grms/goods6

//            10001,20001	10
//            10001,20002	11
//            10001,20003	1
//            10001,20004	2
//            10001,20005	9
//            10001,20006	10
//            10001,20007	8
//            10002,20001	2
//            10002,20002	2
//            10002,20003	3
//            10002,20004	4



      // 10001	20001,20005,20006,20007,20002   经过map处理后输出到
    //    reduce的数据是  10001，200001   1   和   10001.20005   1

//    private static HashMap<String,String> map1=new HashMap<>();
//    private  static HashMap<String,String> map2=new HashMap<>();


   public static class DuplicateDataForResultFirstMapper
            extends Mapper<LongWritable, Text,Text,Text> {

        private Text k2=new Text();
        private Text v2=new Text();

        @Override
        protected void map(LongWritable k1, Text v1, Context context) throws IOException, InterruptedException {

            //             10001	20001,20005,20006,20007,20002

            String[] strs = v1.toString().split("[\t]");

            System.out.println("v1++!!!!!" + v1.toString());

            String[] str1 = strs[1].split("[,]");

            for (String s : str1) {

                    this.k2.set(strs[0]+","+s);
                    this.v2.set(String.valueOf(1));

                    context.write(this.k2,this.v2);

            }




        }
    }

//               10002,20004	4   , 10001,20001	10
    //          经过map处理后输出到
    //       reduce的数据是   10002,20004	4     ,  10001,20001	10


  public   static class DuplicateDataForResultSecondMapper
            extends Mapper<LongWritable, Text,Text,Text> {

        private Text k2=new Text();
        private Text v2=new Text();

        @Override
        protected void map(LongWritable k1, Text v1, Context context) throws IOException, InterruptedException {

            String[] strs = v1.toString().split("[\t]");

            this.k2.set(strs[0]);
            this.v2.set(strs[1]);

            context.write(this.k2,this.v2);
        }
    }



    //    map1:    10001,20001   1
    //    map2:    10001`,20001	10  和  10002,20004	4

    //    10001,20001    [  1, 10]    和     10002,20004	4

  public   static class  DuplicateDataForResultReducer
            extends Reducer<Text,Text,Text, Text> {

        private Text k3=new Text();
        private Text v3=new Text();


        @Override
        protected void reduce(Text k2, Iterable<Text> v2s, Context context) throws IOException, InterruptedException {


            Iterator<Text> it = v2s.iterator();
            Text t = it.next();
            if (it.hasNext())return;

            String[] strs = k2.toString().split("[,]");

            this.k3.set(strs[0]+"\t"+strs[1]);

            this.v3.set(t);

            context.write(this.k3,this.v3);
        }

    }

    @Override
    public int run(String[] strings) throws Exception {

        Configuration conf = this.getConf();
        Path in1 = new Path(conf.get("in1"));
        Path in2 = new Path(conf.get("in2"));
        Path out = new Path(conf.get("out"));


        Job job = Job.getInstance(conf,"数据去重，在推荐结果中去掉用户已购买的商品信息");
        job.setJarByClass(this.getClass());


        //map1端配

        MultipleInputs.addInputPath(job,in1,TextInputFormat.class,DuplicateDataForResultFirstMapper.class);
        MultipleInputs.addInputPath(job,in2,TextInputFormat.class,DuplicateDataForResultSecondMapper.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);



        //reduce端配置
        job.setReducerClass(DuplicateDataForResultReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job,out);



        return job.waitForCompletion(true)?0:1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new DuplicateDataForResult(),args));
    }
}
