package org.itmo.lab3.mapper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.itmo.lab3.Application;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

public class QuantityRowMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

    private static final Logger log = Logger.getLogger(QuantityRowMapper.class.getSimpleName());

    private String csvDelimer;
    private int categoryIndex;
    private int quantityIndex;
    private int expectedColumnCount;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        categoryIndex = conf.getInt(Application.CATEGORY, 2);
        quantityIndex = conf.getInt(Application.QUANTITY, 4);
        csvDelimer = conf.get("CSV_DELIMITER", ",");
        expectedColumnCount = Math.max(categoryIndex, quantityIndex) + 1;
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if (key.get() == 0) return;
        String[] tokens = value.toString().split(csvDelimer);
        if (tokens.length < expectedColumnCount) {
            log.log(Level.WARNING, () ->
                    "Expect " + expectedColumnCount + " columns, but found " + tokens.length
            );
        }
        try {
            String category = tokens[categoryIndex];
            long quantity = Long.parseLong(tokens[quantityIndex]);
            context.write(new Text(category), new LongWritable(quantity));
        } catch (NumberFormatException e) {
            log.log(Level.WARNING, "Can't parse number", e);
        }
    }
}
