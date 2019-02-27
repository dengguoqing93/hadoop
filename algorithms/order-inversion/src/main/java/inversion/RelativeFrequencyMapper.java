package inversion;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 映射器类
 *
 * @author dengguoqing
 * @date 2019/2/27
 */
public class RelativeFrequencyMapper extends
        Mapper<LongWritable, Text, PairOfWords, IntWritable> {
    private int neighborWindow = 2;
    private PairOfWords pair = new PairOfWords();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        neighborWindow = context.getConfiguration().getInt("neighbor.window", 2);
    }

    @Override
    protected void map(LongWritable key, Text value,
                       Context context) throws IOException, InterruptedException {
        String[] tokens = StringUtils.split(value.toString(), " ");
        if (tokens.length < neighborWindow) {
            return;
        }

        for (int i = 0; i < tokens.length; i++) {
            String word = tokens[i];
            pair.setWord(word);
            int start = (i - neighborWindow < 0) ? 0 : i - neighborWindow;
            int end = (i + neighborWindow >= tokens.length) ? tokens.length - 1 : i + neighborWindow;
            for (int j = start; j <= end; j++) {
                if (i == j) {
                    continue;
                }
                pair.setNeighbor(tokens[j]);
                context.write(pair, new IntWritable(1));
            }

            pair.setNeighbor("*");
            context.write(pair, new IntWritable(end - start));
        }
    }
}
