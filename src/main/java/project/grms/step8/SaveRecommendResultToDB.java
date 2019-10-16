package project.grms.step8;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * @program: shuaixiaohuo.java
 * @Description: GRMS
 * @author: ZZZss
 * @Created 2019/10/14 11:06
 */
public class SaveRecommendResultToDB extends Configured implements Tool {


   public static  class SaveRecommendResultToDBMapper
            extends Mapper<LongWritable, Text,Key,NullWritable>{

        private Key k2=new Key();
        private NullWritable v2=NullWritable.get();

        @Override
        protected void map(LongWritable k1, Text v1, Context context) throws IOException, InterruptedException {

            String[] strs = v1.toString().split("[\t]");

            k2.setUid(strs[0]);
            k2.setGid(strs[1]);
            k2.setExp(Integer.parseInt(strs[2]));

            context.write(this.k2,this.v2);

        }
    }







    @Override
    public int run(String[] strings) throws Exception {

        Configuration conf = this.getConf();
        Path in = new Path(conf.get("in"));


        Job job = Job.getInstance(conf,"将推荐结果保存到MySQL数据库中");
        job.setJarByClass(this.getClass());


        //map端配置
        job.setMapperClass(SaveRecommendResultToDBMapper.class);
        job.setMapOutputKeyClass(Key.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job,in);


        //reduce端配置
        job.setOutputKeyClass(Key.class);
        job.setOutputValueClass(NullWritable.class);
        job.setOutputFormatClass(DBOutputFormat.class);

        DBConfiguration.configureDB(
                job.getConfiguration(),
                "com.mysql.cj.jdbc.Driver",
                "jdbc:mysql://192.168.136.133:3306/grms",
                "root",
                "362326"
        );

        DBOutputFormat.setOutput(job,"results","uid","gid","exp");



//
//        // 配置数据的连接信息
//        DBConfiguration.configureDB(
//                job.getConfiguration(),
//                "com.mysql.cj.jdbc.Driver",
//                "jdbc:mysql://192.168.136.133:3306/grms",
//                "root",
//                "362326");
//
//        // 配置数据输出的信息
//        DBOutputFormat.setOutput(job,"results","uid","gid","exp");
//



        return job.waitForCompletion(true)?0:1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new SaveRecommendResultToDB(),args));
    }


}
