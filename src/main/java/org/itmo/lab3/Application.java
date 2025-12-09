package org.itmo.lab3;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.itmo.lab3.reducer.DoubleSumReducer;

import java.io.IOException;

public class Application {

    public static final String TRANSACTION_ID = "TRANSACTION_ID";
    public static final String PRODUCT_ID = "PRODUCT_ID";
    public static final String CATEGORY = "CATEGORY";
    public static final String PRICE = "PRICE";
    public static final String QUANTITY = "QUANTITY";

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

        if (args.length != 2) {
            System.err.println("Usage: Application <input path> <output path>");
            System.err.println("Example: Application /shared/0.csv /shared/output");
            System.exit(-1);
        }

        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);

        JobConf conf = new JobConf();
        FileInputFormat.addInputPath(conf, inputPath);
        FileOutputFormat.setOutputPath(conf, outputPath);

        Job job = Job.getInstance(conf, "Sales revenue");

        job.setJarByClass(Application.class);

        job.setMapperClass(RevenueRowMapper.class);
        job.setReducerClass(DoubleSumReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(SaleInfo.class);

        job.waitForCompletion(true);
    }

}
