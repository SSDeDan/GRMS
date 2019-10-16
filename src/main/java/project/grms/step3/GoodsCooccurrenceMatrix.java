package project.grms.step3;

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
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * @program: shuaixiaohuo.java
 * @Description: GRMS
 * @author: ZZZss
 * @Created 2019/10/10 23:57
 */
public class GoodsCooccurrenceMatrix   extends Configured implements Tool {




    //输入数据是第二步得到的数据

//                        20001	20001
//                       20001	20001
//                       20001	20002
//                       20001	20005
//                       20001	20006
//                       20001	20007
//                       20001	20001
//                       20001	20006
//                       20001	20005



    //处理后输出的结果数据

//             20001	20001:3,20002:2,20005:2,20006:2,20007:1
//             20002	20001:2,20002:3,20005:2,20006:2,20007:2
//             20003	20003:1,20004:1,20006:1
//             20004	20003:1,20004:2,20006:1,20007:1
//             20005	20001:2,20002:2,20005:2,20006:2,20007:1
//             20006	20001:2,20002:2,20003:1,20004:1,20005:2,20006:3,20007:1
//             20007	20001:1,20002:2,20004:1,20005:1,20006:1,20007:3





    public  static class GoodsCooccurrenceMatrixMapper
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


    public  static class  GoodsCooccurrenceMatrixReducer
            extends Reducer<Text,Text,Text,Text> {

        private Text k3=new Text();
        private Text v3=new Text();


        @Override
        protected void reduce(Text k2, Iterable<Text> v2s, Context context) throws IOException, InterruptedException {

            HashMap<String,Integer> map=new HashMap<>();

            for (Text v2 : v2s) {
              map.put(v2.toString(),map.get(v2.toString())==null?1:map.get(v2.toString())+1);
            }

            StringBuffer sb = new StringBuffer();

            Set<Map.Entry<String, Integer>> set = map.entrySet();

            for (Map.Entry<String, Integer> m : set) {
                sb.append(m.getKey()).append(":").append(m.getValue()).append(",");
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


        Job job = Job.getInstance(conf,"计算商品的共现次数(共现矩阵)");
        job.setJarByClass(this.getClass());


        //map端配置
        job.setMapperClass(GoodsCooccurrenceMatrixMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job,in);


        //reduce端配置
        job.setReducerClass(GoodsCooccurrenceMatrixReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job,out);




        return job.waitForCompletion(true)?0:1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new GoodsCooccurrenceMatrix(),args));
    }

}
