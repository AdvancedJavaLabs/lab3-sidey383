package org.itmo.lab3.reducer;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.stream.StreamSupport;

public class DoubleSumReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

    @Override
    public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
        double sum = StreamSupport.stream(values.spliterator(), true)
                .mapToDouble(DoubleWritable::get)
                .sum();
        context.write(key, new DoubleWritable(sum));
    }

}
