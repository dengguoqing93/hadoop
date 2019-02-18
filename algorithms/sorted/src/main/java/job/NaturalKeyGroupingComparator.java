package job;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * 分组比较器
 *
 * @author dengguoqing
 * @date 2019/2/17
 */
public class NaturalKeyGroupingComparator extends WritableComparator {
    protected NaturalKeyGroupingComparator() {
        super(CompositeKey.class, true);
    }
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        CompositeKey ck1 = (CompositeKey) a;
        CompositeKey ck2 = (CompositeKey) b;
        return ck1.getStockSymbol().compareTo(ck2.getStockSymbol());
    }
}
