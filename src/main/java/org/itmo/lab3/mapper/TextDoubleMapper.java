package org.itmo.lab3.mapper;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.itmo.lab3.TextDoubleWritable;

import java.io.IOException;

public class TextDoubleMapper extends Mapper<Text, DoubleWritable, TextDoubleWritable, DoubleWritable> {
    @Override
    protected void map(Text key, DoubleWritable value, Context context) throws IOException, InterruptedException {
        context.write(new TextDoubleWritable(key.toString(), value.get()), value);
    }
}
