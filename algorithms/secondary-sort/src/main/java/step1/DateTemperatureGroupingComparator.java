package step1;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * 定制比较器
 *
 * @author dengguoqing
 * @date 2019/2/16
 */
public class DateTemperatureGroupingComparator extends WritableComparator {

    public DateTemperatureGroupingComparator() {
        super(DateTemperaturePair.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        DateTemperaturePair pair = (DateTemperaturePair) a;
        DateTemperaturePair pair1 = (DateTemperaturePair) b;
        return pair.getYearMonth().compareTo(pair1.getYearMonth());
    }
}
