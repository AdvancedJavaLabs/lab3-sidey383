import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.itmo.lab3.TextDoubleWritable;
import org.itmo.lab3.mapper.QuantityRowMapper;
import org.itmo.lab3.mapper.RevenueRowMapper;
import org.itmo.lab3.mapper.TextDoubleMapper;
import org.itmo.lab3.reducer.DoubleSumReducer;
import org.itmo.lab3.reducer.LongSumReducer;
import org.itmo.lab3.reducer.TextDoubleReducer;

import java.io.IOException;

public class Application {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

        if (args.length < 4) {
            System.err.println("Example: Application /data/0.csv /revenue_output /sort_output /quantity_output");
            System.exit(-1);
        }

        Path inputPath = new Path(args[0]);
        Path revenueOutputFile = new Path(args[1]);
        Path sortOutputFile = new Path(args[2]);
        Path quantityOutputFile = new Path(args[3]);

        JobConf confRevenue = new JobConf();
        FileInputFormat.addInputPath(confRevenue, inputPath);
        FileOutputFormat.setOutputPath(confRevenue, revenueOutputFile);

        JobConf confSort = new JobConf();
        FileInputFormat.addInputPath(confSort, revenueOutputFile);
        FileOutputFormat.setOutputPath(confSort, sortOutputFile);

        JobConf confQuantity = new JobConf();
        FileInputFormat.addInputPath(confQuantity, inputPath);
        FileOutputFormat.setOutputPath(confQuantity, quantityOutputFile);

        Job revenueJob = Job.getInstance(confRevenue, "Sales revenue");

        revenueJob.setInputFormatClass(TextInputFormat.class);

        revenueJob.setJarByClass(Application.class);
        revenueJob.setMapperClass(RevenueRowMapper.class);
        revenueJob.setReducerClass(DoubleSumReducer.class);

        revenueJob.setOutputFormatClass(SequenceFileOutputFormat.class);
        revenueJob.setOutputKeyClass(Text.class);
        revenueJob.setOutputValueClass(DoubleWritable.class);

        Job sortJob = Job.getInstance(confSort, "Category sort");

        sortJob.setInputFormatClass(SequenceFileInputFormat.class);

        sortJob.setJarByClass(Application.class);
        sortJob.setMapperClass(TextDoubleMapper.class);
        sortJob.setMapOutputKeyClass(TextDoubleWritable.class);
        sortJob.setMapOutputValueClass(DoubleWritable.class);

        sortJob.setReducerClass(TextDoubleReducer.class);
        sortJob.setOutputFormatClass(TextOutputFormat.class);
        sortJob.setOutputKeyClass(Text.class);
        sortJob.setOutputValueClass(DoubleWritable.class);

        Job quantityJob = Job.getInstance(confQuantity, "Sales quantity");

        quantityJob.setInputFormatClass(TextInputFormat.class);

        quantityJob.setMapperClass(QuantityRowMapper.class);
        quantityJob.setReducerClass(LongSumReducer.class);

        quantityJob.setOutputFormatClass(TextOutputFormat.class);
        quantityJob.setOutputKeyClass(Text.class);
        quantityJob.setOutputValueClass(LongWritable.class);

        revenueJob.waitForCompletion(true);
        sortJob.waitForCompletion(true);
        quantityJob.waitForCompletion(true);
    }

}
