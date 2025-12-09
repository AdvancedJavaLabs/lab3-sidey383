package org.itmo.lab3;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Partitioner;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Comparator;
import java.util.function.Function;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class TextDoublePair implements Writable, WritableComparable<TextDoublePair> {

    private static final Comparator<TextDoublePair> COMPARATOR = Comparator
            .comparing((Function<TextDoublePair, String>) pair -> pair.text)
            .thenComparing(pair -> pair.value)
            .reversed();

    private String text;
    private double value;

    @Override
    public int compareTo(TextDoublePair o) {
        return COMPARATOR.compare(this, o);
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

    public static class PairPartitioner extends Partitioner<TextDoublePair, Text> {
        @Override
        public int getPartition(TextDoublePair textDoublePair, Text text, int numPartitions) {
            return Math.abs(textDoublePair.getText().hashCode()) % numPartitions;
        }
    }

    public static class GroupComparator implements RawComparator<TextDoublePair> {


        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return 0;
        }

        @Override
        public int compare(TextDoublePair o1, TextDoublePair o2) {
            return o1.text.compareTo(o2.text);
        }
    }
}
