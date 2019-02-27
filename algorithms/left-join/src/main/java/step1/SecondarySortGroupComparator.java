package step1;

import edu.umd.cloud9.io.pair.PairOfStrings;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.RawComparator;

/**
 * 对自然键分组
 *
 * @author dengguoqing
 * @date 2019/2/26
 */
public class SecondarySortGroupComparator implements RawComparator<PairOfStrings> {
    @Override
    public int compare(byte[] bytes, int i, int i1, byte[] bytes1, int i2, int i3) {

        PairOfStrings a = new PairOfStrings();
        PairOfStrings b = new PairOfStrings();
        try (DataInputBuffer buffer = new DataInputBuffer()) {
            buffer.reset(bytes, i, i1);
            a.readFields(buffer);
            buffer.reset(bytes1, i2, i3);
            b.readFields(buffer);
            return compare(a, b);
        } catch (Exception ex) {
            return -1;
        }
    }

    @Override
    public int compare(PairOfStrings o1, PairOfStrings o2) {
        return o1.getLeftElement().compareTo(o2.getLeftElement());
    }

}
