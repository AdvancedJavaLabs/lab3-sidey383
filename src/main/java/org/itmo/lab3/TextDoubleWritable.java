package org.itmo.lab3;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class TextDoubleWritable implements Writable, WritableComparable<TextDoubleWritable> {

    private String text;
    private double value;

    @Override
    public int compareTo(TextDoubleWritable o) {
        int result = Double.compare(value, o.value);
        if (result != 0) return -result;

        return text.compareTo(o.text);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(text);
        dataOutput.writeDouble(value);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        text = dataInput.readUTF();
        value = dataInput.readDouble();
    }

}
