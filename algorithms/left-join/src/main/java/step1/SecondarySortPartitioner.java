package step1;

import edu.umd.cloud9.io.pair.PairOfStrings;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 对自然键进行分区
 *
 * @author dengguoqing
 * @date 2019/2/26
 */
public class SecondarySortPartitioner extends Partitioner<PairOfStrings, Object> {

    @Override
    public int getPartition(PairOfStrings key, Object value,
                            int numberOfPartitions) {
        return (key.getLeftElement().hashCode() & Integer.MAX_VALUE) % numberOfPartitions;
    }
}
