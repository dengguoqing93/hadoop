package job;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.time.LocalDate;

/**
 * @author dengguoqing
 * @date 2019/2/17
 */
public class NaturalValue implements WritableComparable<NaturalValue> {
    private long timestamp;
    private double price;

    public static NaturalValue copy(NaturalValue value) {
        return new NaturalValue(value.timestamp, value.price);
    }

    public NaturalValue(long timestamp, double price) {
        set(timestamp, price);
    }

    public NaturalValue() {
    }

    public void set(long timestamp, double price) {
        this.timestamp = timestamp;
        this.price = price;
    }

    public long getTimestamp() {
        return this.timestamp;
    }

    public double getPrice() {
        return this.price;
    }

    /**
     * Deserializer the point from the underlying data.
     *
     * @param in a DataInput object to read the point from.
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        this.timestamp = in.readLong();
        this.price = in.readDouble();
    }

    /**
     * Convert a binary data into NaturalValue
     *
     * @param in A DataInput object to read from.
     * @return A NaturalValue object
     * @throws IOException
     */
    public static NaturalValue read(DataInput in) throws IOException {
        NaturalValue value = new NaturalValue();
        value.readFields(in);
        return value;
    }

    public String getDate() {
        return LocalDate.ofEpochDay(timestamp).toString();
    }



    public NaturalValue(NaturalValue value){
        new NaturalValue(value.timestamp, value.price);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(this.timestamp);
        out.writeDouble(this.price);

    }

    /**
     * Used in sorting the data in the reducer
     */
    @Override
    public int compareTo(NaturalValue data) {
        if (this.timestamp < data.timestamp) {
            return -1;
        } else if (this.timestamp > data.timestamp) {
            return 1;
        } else {
            return 0;
        }
    }

    @Override
    public int hashCode() {
        return 163 + Long.valueOf(timestamp).hashCode() + Double.valueOf(
                price).hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof NaturalValue){
            NaturalValue other = (NaturalValue) obj;
            return this.timestamp == other.getTimestamp() && this.price == other.price;
        }
        return false;
    }

    @Override
    public String toString() {
        return super.toString();
    }
}
