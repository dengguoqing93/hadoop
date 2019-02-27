package step1;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 一个（String stockSymbol，long timestamp）对
 * 在stockSymbol字段上完成一次分组
 * 将相同类型的所有数据分为一组，然后洗牌阶段的二次排序
 * 使用timestamp long分量对数据点排序
 * 是的它们到达归约器时已经分区而且是有序的
 *
 * @author dengguoqing
 * @date 2019/2/17
 */
public class CompositeKey implements Writable, WritableComparable<CompositeKey> {

    /**
     * 股票代码
     */
    private String stockSymbol;
    /**
     * 日期
     */
    private long timestamp;

    public CompositeKey(String stockSymbol, long timestamp) {
        set(stockSymbol, timestamp);
    }

    public CompositeKey() {
    }

    public void set(String stockSymbol, long timestamp) {
        this.stockSymbol = stockSymbol;
        this.timestamp = timestamp;
    }

    public String getStockSymbol() {
        return stockSymbol;
    }

    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public int compareTo(CompositeKey o) {

        if (this.stockSymbol.compareTo(o.stockSymbol) != 0) {
            return this.stockSymbol.compareTo(o.stockSymbol);
        } else if (this.timestamp != o.timestamp) {
            return timestamp > o.timestamp ? 1 : -1;
        } else {
            return 0;
        }
    }


    @Override
    public void write(DataOutput output) throws IOException {
        output.writeUTF(stockSymbol);
        output.writeLong(timestamp);
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        stockSymbol = input.readUTF();
        timestamp = input.readLong();
    }

    @Override
    public int hashCode() {
        return stockSymbol.hashCode() + 163 + Long.valueOf(timestamp).hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof CompositeKey) {
            CompositeKey other = (CompositeKey) obj;
            return stockSymbol.equals(
                    other.stockSymbol) && timestamp == other.getTimestamp();
        }
        return false;
    }

    @Override
    public String toString() {
        return super.toString();
    }
}
