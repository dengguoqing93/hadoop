package inversion;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 归约器类
 *
 * @author dengguoqing
 * @date 2019/2/27
 */
public class RelativeFrequencyReducer extends
        Reducer<PairOfWords, IntWritable, PairOfWords, DoubleWritable> {

    private double totalCount = 0;
    private String currentWord = "NOT_DEFINED";

    private final String Start = "*";

    @Override
    protected void reduce(PairOfWords key, Iterable<IntWritable> values,
                          Context context) throws IOException, InterruptedException {
        if (Start.equals(key.getRightElement())) {
            if (key.getLeftElement().equals(currentWord)) {
                totalCount += getTotalCount(values);
            } else {
                currentWord = key.getLeftElement();
                totalCount = getTotalCount(values);
            }
        } else {
            int count = getTotalCount(values);
            double relativeCount = count / totalCount;
            context.write(key, new DoubleWritable(relativeCount));
        }
    }

    private int getTotalCount(Iterable<IntWritable> values) {
        int count = 0;
        for (IntWritable value : values) {
            count += value.get();
        }
        return count;
    }
}
