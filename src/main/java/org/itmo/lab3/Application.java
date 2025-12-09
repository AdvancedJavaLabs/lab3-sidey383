package org.itmo.lab3;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

public class Application {

    public static final String TRANSACTION_ID = "TRANSACTION_ID";
    public static final String PRODUCT_ID = "PRODUCT_ID";
    public static final String CATEGORY = "CATEGORY";
    public static final String PRICE = "PRICE";
    public static final String QUANTITY = "QUANTITY";

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        var conf = new JobConf();
        conf.set("mapreduce.framework.name", "local");
        conf.set("fs.defaultFS", "file:///C://Users/sidey/IdeaProjects/lab3-sidey383/data/fs/");
        conf.setInt(CATEGORY, 2);
        conf.setInt(PRICE, 3);
        conf.setInt(QUANTITY, 4);

        FileInputFormat.addInputPath(conf, new Path("file:///C://Users/sidey/IdeaProjects/lab3-sidey383/0.csv"));
        FileOutputFormat.setOutputPath(conf, new Path("file:///C://Users/sidey/IdeaProjects/lab3-sidey383/data/output.txt"));

        Job job = Job.getInstance(conf, "Sales Analysis Local");
        job.setMapperClass(SalesRowMapper.class);
        job.setReducerClass(CategoryRevenueReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        job.waitForCompletion(true);
    }

}
