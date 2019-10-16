package project.grms.step9;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import project.grms.step1.UserBuyGoodsList;
import project.grms.step2.GoodsCooccurrenceList;
import project.grms.step3.GoodsCooccurrenceMatrix;
import project.grms.step4.UserBuyGoodsVector;
import project.grms.step5.MultiplyGoodsMatrixAndUserVector1;
import project.grms.step5.MyComparator;
import project.grms.step5.Mypartitioner;
import project.grms.step5.superKey;
import project.grms.step6.MakeSumForMultiplication;
import project.grms.step7.DuplicateDataForResult;
import project.grms.step8.Key;
import project.grms.step8.SaveRecommendResultToDB;

/**
 * @program: shuaixiaohuo.java
 * @Description: GRMS
 * @author: ZZZss
 * @Created 2019/10/15 21:10
 */
public class GoodsRecommendationManagementSystemJobController extends Configured implements Tool {


    public static void main(String[] args) throws Exception{
        System.exit(ToolRunner.run(new GoodsRecommendationManagementSystemJobController(),args));
    }

    @Override
    public int run(String[] strings) throws Exception {

        Configuration conf=this.getConf();

        Path in=new Path(conf.get("in"));  // /grms/rawdata/data.txt
        Path out1=new Path(conf.get("out1"));   //1的输出，是2和4和7的输入
        Path out2=new Path(conf.get("out2"));   //2的输出，是3的输入
        Path out3=new Path(conf.get("out3"));   //3的输出，5的输入
        Path out4=new Path(conf.get("out4"));   //4的输出，是5的输入
        Path out5=new Path(conf.get("out5"));   //5的输出，是6的输入
        Path out6=new Path(conf.get("out6"));   //6的输出， 是7的输入
        Path out7=new Path(conf.get("out7"));   //7的输出，是8的输入


        //作业配置1
        Job job1 = Job.getInstance(conf,"作业1：计算用户购买商品的列表");
        job1.setJarByClass(this.getClass());


        //map端配置
        job1.setMapperClass(UserBuyGoodsList.UserBuyGoodsListMapper.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job1,in);


        //reduce端配置
        job1.setReducerClass(UserBuyGoodsList.UserBuyGoodsListReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        job1.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job1,out1);

        //--------------------------


        //作业配置2


        Job job2 = Job.getInstance(conf,"作业2：计算商品的共现关系");
        job2.setJarByClass(this.getClass());


        //map端配置
        job2.setMapperClass(GoodsCooccurrenceList.GoodsCooccurrenceListMapper.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job2,out1);


        //reduce端配置
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        job2.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job2,out2);

        //--------------------------------------



        //作业配置3

        Job job3 = Job.getInstance(conf,"作业3：计算商品的共现次数(共现矩阵)");
        job3.setJarByClass(this.getClass());


        //map端配置
        job3.setMapperClass(GoodsCooccurrenceMatrix.GoodsCooccurrenceMatrixMapper.class);
        job3.setMapOutputKeyClass(Text.class);
        job3.setMapOutputValueClass(Text.class);
        job3.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job3,out2);


        //reduce端配置
        job3.setReducerClass(GoodsCooccurrenceMatrix.GoodsCooccurrenceMatrixReducer.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);
        job3.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job3,out3);


        //----------------------


        //作业配置4


        Job job4 = Job.getInstance(conf,"作业4：计算用户的购买向量");
        job4.setJarByClass(this.getClass());


        //map端配置
        job4.setMapperClass(UserBuyGoodsVector.UserBuyGoodsVectorMapper.class);
        job4.setMapOutputKeyClass(Text.class);
        job4.setMapOutputValueClass(Text.class);
        job4.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job4,out1);


        //reduce端配置
        job4.setReducerClass(UserBuyGoodsVector.UserBuyGoodsVectorReducer.class);
        job4.setOutputKeyClass(Text.class);
        job4.setOutputValueClass(Text.class);
        job4.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job4,out4);

        //----------------------------



        //作业配置5

        Job job5 = Job.getInstance(conf,"作业5：商品共现矩阵乘以用户购买向量，形成临时的推荐结果");
        job5.setJarByClass(this.getClass());



        MultipleInputs.addInputPath(job5,out3,KeyValueTextInputFormat.class, MultiplyGoodsMatrixAndUserVector1.MultiplyGoodsMatrixAndUserVector1FirstMapper.class);
        MultipleInputs.addInputPath(job5,out4,KeyValueTextInputFormat.class, MultiplyGoodsMatrixAndUserVector1.MultiplyGoodsMatrixAndUserVector1SecondMapper.class);

        job5.setMapOutputKeyClass(superKey.class);
        job5.setMapOutputValueClass(Text.class);


        //reduce端配置
        job5.setReducerClass(MultiplyGoodsMatrixAndUserVector1.MultiplyGoodsMatrixAndUserVector1Reducer.class);
        job5.setOutputKeyClass(Text.class);
        job5.setOutputValueClass(IntWritable.class);
        job5.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job5,out5);


        //设置分区器
        job5.setPartitionerClass(Mypartitioner.class);

        //设置分组器
        job5.setGroupingComparatorClass(MyComparator.class);



        //-----------------------------


        //作业配置6


        Job job6 = Job.getInstance(conf,"作业6：对第5步计算的推荐的零散结果进行求和");
        job6.setJarByClass(this.getClass());


        //map1端配置
        job6.setMapperClass(MakeSumForMultiplication.MakeSumForMultiplicationMapper.class);
        job6.setMapOutputKeyClass(Text.class);
        job6.setMapOutputValueClass(IntWritable.class);
        job6.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job6,out5);


        //reduce端配置
        job6.setReducerClass(MakeSumForMultiplication.MakeSumForMultiplicationReducer.class);
        job6.setOutputKeyClass(Text.class);
        job6.setOutputValueClass(IntWritable.class);
        job6.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job6,out6);


        //--------------------------



        //作业配置7




        Job job7 = Job.getInstance(conf,"作业7：数据去重，在推荐结果中去掉用户已购买的商品信息");
        job7.setJarByClass(this.getClass());


        //map1端配

        MultipleInputs.addInputPath(job7,out1,TextInputFormat.class, DuplicateDataForResult.DuplicateDataForResultFirstMapper.class);
        MultipleInputs.addInputPath(job7,out6,TextInputFormat.class, DuplicateDataForResult.DuplicateDataForResultSecondMapper.class);

        job7.setMapOutputKeyClass(Text.class);
        job7.setMapOutputValueClass(Text.class);

        //reduce端配置
        job7.setReducerClass(DuplicateDataForResult.DuplicateDataForResultReducer.class);
        job7.setOutputKeyClass(Text.class);
        job7.setOutputValueClass(Text.class);
        job7.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job7,out7);


        //--------------------------

        //z作业配置8


        Job job8 = Job.getInstance(conf,"作业8：将推荐结果保存到MySQL数据库中");
        job8.setJarByClass(this.getClass());


        //map端配置
        job8.setMapperClass(SaveRecommendResultToDB.SaveRecommendResultToDBMapper.class);
        job8.setMapOutputKeyClass(Key.class);
        job8.setMapOutputValueClass(NullWritable.class);
        job8.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job8,out7);


        //reduce端配置
        job8.setOutputKeyClass(Key.class);
        job8.setOutputValueClass(NullWritable.class);
        job8.setOutputFormatClass(DBOutputFormat.class);


        DBConfiguration.configureDB(
                job8.getConfiguration(),
                "com.mysql.cj.jdbc.Driver",
                "jdbc:mysql://192.168.136.1:3306/grms",
                "root",
                "362326"
        );

        DBOutputFormat.setOutput(job8,"results","uid","gid","exp");




        //将JOB变为可控制的job
        ControlledJob cj1 = new ControlledJob(conf);
        cj1.setJob(job1);

        ControlledJob cj2 = new ControlledJob(conf);
        cj2.setJob(job2);

        ControlledJob cj3 = new ControlledJob(conf);
        cj3.setJob(job3);

        ControlledJob cj4 = new ControlledJob(conf);
        cj4.setJob(job4);

        ControlledJob cj5 = new ControlledJob(conf);
        cj5.setJob(job5);

        ControlledJob cj6 = new ControlledJob(conf);
        cj6.setJob(job6);

        ControlledJob cj7 = new ControlledJob(conf);
        cj7.setJob(job7);

        ControlledJob cj8 = new ControlledJob(conf);
        cj8.setJob(job8);


        //添加每个job的依赖
        cj2.addDependingJob(cj1);
        cj3.addDependingJob(cj2);
        cj4.addDependingJob(cj1);
        cj5.addDependingJob(cj3);
        cj5.addDependingJob(cj4);
        cj6.addDependingJob(cj5);
        cj7.addDependingJob(cj1);
        cj7.addDependingJob(cj6);
        cj8.addDependingJob(cj7);


        JobControl jc=new JobControl("作业流控制");
        jc.addJob(cj1);
        jc.addJob(cj2);
        jc.addJob(cj3);
        jc.addJob(cj4);
        jc.addJob(cj5);
        jc.addJob(cj6);
        jc.addJob(cj7);
        jc.addJob(cj8);


        // 提交作业
        Thread t=new Thread(jc);
        t.start();

        do{
            for(ControlledJob j: jc.getRunningJobList()){
                j.getJob().monitorAndPrintJob();
            }
        }while(!jc.allFinished());




        return 0;
    }
}
