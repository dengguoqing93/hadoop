package inversion;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

import static java.util.Objects.hash;

/**
 * 定制分区器实现
 *
 * @author dengguoqing
 * @date 2019/2/27
 */
public class OrderInversionPartitioner extends Partitioner<PairOfWords, IntWritable> {
    @Override
    public int getPartition(PairOfWords pairOfWords, IntWritable intWritable, int i) {
        String leftWord = pairOfWords.getLeftElement();
        return Math.abs(hash(leftWord)%i);
    }
}
