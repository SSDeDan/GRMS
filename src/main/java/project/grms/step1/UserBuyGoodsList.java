package project.grms.step1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * @program: shuaixiaohuo.java
 * @Description: GRMS
 * @author: ZZZss
 * @Created 2019/10/10 14:48
 */
public class UserBuyGoodsList extends Configured implements Tool {


//    原始数据
//                    10001	20001	1
//                    10001	20002	1
//                    10001	20005	1
//                    10001	20006	1
//                    10001	20007	1
//                    10002	20003	1
//                    10002	20004	1
//                    10002	20006	1



//处理后结果数据

//         10001	20001,20005,20006,20007,20002
//          10002	20006,20003,20004
//          10003	20002,20007
//          10004	20001,20002,20005,20006
//          10005	20001
//          10006	20004,20007







    public static class  UserBuyGoodsListMapper
            extends Mapper<LongWritable, Text,Text,Text>{

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


    public static class  UserBuyGoodsListReducer
            extends Reducer<Text,Text,Text,Text> {

        private Text k3=new Text();
        private Text v3=new Text();

        @Override
        protected void reduce(Text k2, Iterable<Text> v2s, Context context) throws IOException, InterruptedException {

            StringBuffer sb = new StringBuffer();
            for (Text v2 : v2s) {

                sb.append(v2).append(",");

            }
            this.k3.set(k2.toString());
            this.v3.set(sb.substring(0,sb.length()-1));
            context.write(this.k3,this.v3);

        }
    }


    @Override
    public int run(String[] strings) throws Exception {

        Configuration conf = this.getConf();
        Path in = new Path(conf.get("in"));
        Path out = new Path(conf.get("out"));


        Job job = Job.getInstance(conf,"计算用户购买商品的列表");
        job.setJarByClass(this.getClass());


        //map端配置
        job.setMapperClass(UserBuyGoodsListMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job,in);


        //reduce端配置
        job.setReducerClass(UserBuyGoodsListReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job,out);




        return job.waitForCompletion(true)?0:1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new UserBuyGoodsList(),args));
    }


}
