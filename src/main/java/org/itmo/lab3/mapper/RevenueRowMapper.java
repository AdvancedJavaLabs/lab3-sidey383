package org.itmo.lab3.mapper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

public class RevenueRowMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

    private static final Logger log = Logger.getLogger(RevenueRowMapper.class.getSimpleName());

    private String csvDelimer;
    private int categoryIndex;
    private int priceIndex;
    private int quantityIndex;
    private int expectedColumnCount;

    @Override
    protected void setup(Context context) {
        Configuration conf = context.getConfiguration();
        categoryIndex = conf.getInt("CATEGORY", 2);
        priceIndex = conf.getInt("PRICE", 3);
        quantityIndex = conf.getInt("QUANTITY", 4);
        csvDelimer = conf.get("CSV_DELIMITER", ",");
        expectedColumnCount = Math.max(categoryIndex, Math.max(priceIndex, quantityIndex)) + 1;
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
            double price = Double.parseDouble(tokens[priceIndex]);
            long quantity = Long.parseLong(tokens[quantityIndex]);
            context.write(new Text(category), new DoubleWritable(price * quantity));
        } catch (NumberFormatException e) {
            log.log(Level.WARNING, "Can't parse number", e);
        }

    }
}
