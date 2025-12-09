package org.itmo.lab3.reducer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.stream.StreamSupport;

public class LongSumReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

    @Override
    public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        long sum = StreamSupport.stream(values.spliterator(), true)
                .mapToLong(LongWritable::get)
                .sum();
        context.write(key, new LongWritable(sum));
    }
}
