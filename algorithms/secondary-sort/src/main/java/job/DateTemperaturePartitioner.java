package job;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 定制分区器
 *
 * @author dengguoqing
 * @date 2019/2/16
 */
public class DateTemperaturePartitioner extends Partitioner<DateTemperaturePair, Text> {
    @Override
    public int getPartition(DateTemperaturePair pair, Text text, int i) {
        return Math.abs(pair.getYearMonth().hashCode() % i);
    }
}
