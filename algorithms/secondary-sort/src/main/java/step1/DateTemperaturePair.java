package step1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 排序对象
 *
 * @author dengguoqing
 * @date 2019/2/16
 */
public class DateTemperaturePair implements Writable,
        WritableComparable<DateTemperaturePair> {

    /**
     * 自然键
     */
    private Text yearMonth = new Text();
    private Text day = new Text();
    /**
     * 次键
     */
    private IntWritable temperature = new IntWritable();

    public DateTemperaturePair() {
    }

    public DateTemperaturePair(Text yearMonth, Text day,
                               IntWritable temperature) {
        this.yearMonth = yearMonth;
        this.day = day;
        this.temperature = temperature;
    }

    public Text getYearMonth() {
        return yearMonth;
    }

    public void setYearMonth(String yearMonth) {
        this.yearMonth = new Text(yearMonth);
    }

    public Text getDay() {
        return day;
    }

    public void setDay(String day) {
        this.day = new Text(day);
    }

    public IntWritable getTemperature() {
        return temperature;
    }

    public void setTemperature(String temperature) {
        this.temperature = new IntWritable(Integer.valueOf(temperature));
    }


    public void setTemperature(int temperature) {
        this.temperature = new IntWritable(temperature);
    }

    @Override
    public int compareTo(DateTemperaturePair pair) {
        int compareValue = yearMonth.compareTo(pair.getYearMonth());
        if (compareValue == 0) {
            compareValue = temperature.compareTo(pair.getTemperature());
        }
        /*
        降序排序
         */
        return -compareValue;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        yearMonth.write(dataOutput);
        temperature.write(dataOutput);
        day.write(dataOutput);

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        yearMonth.readFields(dataInput);
        temperature.readFields(dataInput);
        day.readFields(dataInput);

    }
}
