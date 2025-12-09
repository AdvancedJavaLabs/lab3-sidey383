package org.itmo.lab3.reducer;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.itmo.lab3.TextDoubleWritable;

import java.io.IOException;

public class TextDoubleReducer extends Reducer<TextDoubleWritable, DoubleWritable, Text, DoubleWritable> {

    @Override
    protected void reduce(TextDoubleWritable key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
        for (DoubleWritable v : values) {
            context.write(new Text(key.getText()), v);
        }
    }
}
