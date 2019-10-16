package project.grms.step6;

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
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import project.grms.step5.MultiplyGoodsMatrixAndUserVector;

import java.io.IOException;
import java.util.HashMap;

/**
 * @program: shuaixiaohuo.java
 * @Description: GRMS
 * @author: ZZZss
 * @Created 2019/10/11 19:44
 */
public class MakeSumForMultiplication  extends Configured implements Tool {



//输入数据
//             10004,20006	2
//            10004,20006	2
//            10004,20007	2
//            10004,20007	1
//            10004,20007	1
//            10004,20007	1
//            10005,20001	3
//            10005,20002	2
//            10005,20005	2
//            10005,20006	2
//            10005,20007	1




    //输出数据


//                10001,20001	10
    //			10001,20002	11
    //			10001,20003	1
    //			10001,20004	2
    //			10001,20005	9
    //			10001,20006	10
    //			10001,20007	8
    //			10002,20001	2
    //			10002,20002	2
    //			10002,20003	3
    //			10002,20004	4
    //			10002,20005	2
    //			10002,20006	5
    //			10002,20007	2
    //			10003,20001	3
    //			10003,20002	5



   public static class MakeSumForMultiplicationMapper
            extends Mapper<LongWritable, Text,Text,IntWritable> {

        private Text k2=new Text();
        private IntWritable v2=new IntWritable();

        @Override
        protected void map(LongWritable k1, Text v1, Context context) throws IOException, InterruptedException {

            String[] strs = v1.toString().split("\t");
            this.k2.set(strs[0]);
            this.v2.set(Integer.parseInt(strs[1]));
            context.write(this.k2,this.v2);

        }
    }


  public   static class MakeSumForMultiplicationReducer
            extends Reducer<Text,IntWritable,Text,IntWritable> {

        private Text k3=new Text();
        private IntWritable v3=new IntWritable();


        @Override
        protected void reduce(Text k2, Iterable<IntWritable> v2s, Context context) throws IOException, InterruptedException {

            int sum=0;

            for (IntWritable v2 : v2s) {
               sum+=v2.get();

            }

            this.k3.set(k2.toString());
            this.v3.set(sum);
            context.write(this.k3,this.v3);

        }

    }

    @Override
    public int run(String[] strings) throws Exception {

        Configuration conf = this.getConf();
        Path in = new Path(conf.get("in"));
        Path out = new Path(conf.get("out"));


        Job job = Job.getInstance(conf,"对第5步计算的推荐的零散结果进行求和");
        job.setJarByClass(this.getClass());


        //map1端配置
        job.setMapperClass(MakeSumForMultiplicationMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job,in);


        //reduce端配置
        job.setReducerClass(MakeSumForMultiplicationReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job,out);




        return job.waitForCompletion(true)?0:1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new MakeSumForMultiplication(),args));
    }
}
