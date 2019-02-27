package average;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 一个（时间戳，值）对
 *
 * @author dengguoqing
 * @date 2019/2/27
 */
public class TimeSeriesData implements WritableComparable<TimeSeriesData> {

    private Long timestamp;
    private double value;

    @Override
    public int compareTo(TimeSeriesData other) {
        return timestamp.compareTo(other.timestamp);
    }

    @Override
    public void write(DataOutput output) throws IOException {
        output.writeLong(timestamp);
        output.writeDouble(value);

    }

    @Override
    public void readFields(DataInput input) throws IOException {
        this.timestamp  = input.readLong();
        this.value  = input.readDouble();
    }



    public TimeSeriesData() {
    }

    public static TimeSeriesData copy(TimeSeriesData that) {
        return new TimeSeriesData(that.timestamp, that.value);
    }

    public TimeSeriesData(Long timestamp, double value) {
        set(timestamp, value);
    }

    private void set(Long timestamp, double value) {
        this.timestamp = timestamp;
        this.value = value;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public double getValue() {
        return value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        TimeSeriesData that = (TimeSeriesData) o;

        if (Double.compare(that.value, value) != 0) {
            return false;
        }
        return timestamp.equals(that.timestamp);
    }

    @Override
    public int hashCode() {
        int result;
        long temp;
        result = timestamp.hashCode();
        temp = Double.doubleToLongBits(value);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        return result;
    }


    @Override
    public String toString() {
        return "("+timestamp+","+value+")";
    }
}
