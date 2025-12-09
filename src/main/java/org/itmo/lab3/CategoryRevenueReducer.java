package org.itmo.lab3;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.w3c.dom.Text;

import java.io.IOException;

public class CategoryRevenueReducer extends Reducer<Text, SaleInfo, Text, DoubleWritable> {

    @Override
    public void reduce(Text key, Iterable<SaleInfo> values, Context context) throws IOException, InterruptedException {

        double sum = 0.0;
        for (var value : values) {
            sum += value.getRevenue();
        }
        context.write(key, new DoubleWritable(sum));
    }

}
