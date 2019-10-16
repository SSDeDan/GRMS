package project.grms.step2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * @program: shuaixiaohuo.java
 * @Description: GRMS
 * @author: ZZZss
 * @Created 2019/10/10 19:39
 */
public class GoodsCooccurrenceList extends Configured implements Tool {


//    输入数据

//                   10001	20001,20005,20006,20007,20002
//                  10002	20006,20003,20004
//                  10003	20002,20007
//                  10004	20001,20002,20005,20006
//                  10005	20001
//                  10006	20004,20007




    //处理后输出结果数据

//                     20001	20001
//                       20001	20001
//                       20001	20002
//                       20001	20005
//                       20001	20006
//                       20001	20007
//                       20001	20001
//                       20001	20006
//                       20001	20005


    public  static class GoodsCooccurrenceListMapper
            extends Mapper<LongWritable, Text,Text,Text> {

        private  Text k2=new Text();
        private  Text v2=new Text();

        @Override
        protected void map(LongWritable k1, Text v1, Context context) throws IOException, InterruptedException {

            String[] strs = v1.toString().split("\t");
            String[] strs1 = strs[1].split("[,]");
            for (int i=0;i<=strs1.length-1;i++){
                for (int j=0;j<=strs1.length-1;j++){

                    this.k2.set(strs1[i]);
                    this.v2.set(strs1[j]);
                    context.write(this.k2,this.v2);
                }
            }


        }
    }




    @Override
    public int run(String[] strings) throws Exception {

        Configuration conf = this.getConf();
        Path in = new Path(conf.get("in"));
        Path out = new Path(conf.get("out"));


        Job job = Job.getInstance(conf,"计算商品的共现关系");
        job.setJarByClass(this.getClass());


        //map端配置
        job.setMapperClass(GoodsCooccurrenceListMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job,in);


        //reduce端配置
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job,out);




        return job.waitForCompletion(true)?0:1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new GoodsCooccurrenceList(),args));
    }

}
