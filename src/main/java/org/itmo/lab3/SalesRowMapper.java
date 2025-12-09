package org.itmo.lab3;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

public class SalesRowMapper extends Mapper<LongWritable , Text, Text, SaleInfo> {

    private static final Logger log = Logger.getLogger(SalesRowMapper.class.getSimpleName());

    private String csvDelimer;
    private int categoryIndex;
    private int priceIndex;
    private int quantityIndex;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        var conf = context.getConfiguration();
        categoryIndex = conf.getInt(Application.CATEGORY, 2);
        priceIndex = conf.getInt(Application.PRICE, 3);
        quantityIndex = conf.getInt(Application.QUANTITY, 4);
        csvDelimer = conf.get("CSV_DELIMITER", ",");
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if (key.get() == 0) return;
        String[] tokens = value.toString().split(csvDelimer);
        int expectedColumnCount = Math.max(categoryIndex, Math.max(priceIndex, quantityIndex));
        if (tokens.length > expectedColumnCount) {
            try {
                String category = tokens[categoryIndex];
                double price = Double.parseDouble(tokens[priceIndex]);
                long quantity = Long.parseLong(tokens[quantityIndex]);
                context.write(new Text(category), new SaleInfo(price, quantity));
            } catch (NumberFormatException e) {
                log.log(Level.WARNING, "Can't parse number", e);
            }
        } else {
            log.log(Level.WARNING, () -> "Expect %d columns, but found %d".formatted(expectedColumnCount, tokens.length));
        }
        
    }
}
