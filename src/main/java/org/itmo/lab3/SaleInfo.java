package org.itmo.lab3;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class SaleInfo implements Writable {

    private double price;
    private long quantity;

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeDouble(price);
        out.writeLong(quantity);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        price = in.readDouble();
        quantity = in.readLong();
    }

    public double getRevenue() {
        return price * quantity;
    }

}
