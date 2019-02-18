package job;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * 比较器类
 *
 * @author dengguoqing
 * @date 2019/2/17
 */
public class CompositeKeyComparator extends WritableComparator {

    protected CompositeKeyComparator() {
        super(CompositeKey.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        CompositeKey ck1 = (CompositeKey) a;
        CompositeKey ck2 = (CompositeKey) b;
        return ck1.compareTo(ck2);
    }
}
