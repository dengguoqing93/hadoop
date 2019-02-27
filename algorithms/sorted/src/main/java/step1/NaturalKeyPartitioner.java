package step1;

import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 自然键分区器
 * 映射阶段的数据发送到洗牌阶段之前，
 * NaturalKeyPartitioner对输出分区
 *
 * @author dengguoqing
 * @date 2019/2/17
 */
public class NaturalKeyPartitioner extends Partitioner<CompositeKey, NaturalValue> {
    @Override
    public int getPartition(CompositeKey compositeKey, NaturalValue naturalValue, int i) {
        return Math.abs((int) (hash(compositeKey.getStockSymbol()) % i));
    }

    /**
     * adapted from String.hashCode()
     */
    static long hash(String str) {
        long h = 1125899906842597L;
        int length = str.length();
        for (int i = 0; i < length; i++) {
            h = 31 * h + str.charAt(i);
        }
        return h;
    }

}
