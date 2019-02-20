package aggregate;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 聚合归约
 *
 * @author dengguoqing
 * @date 2019/2/20
 */
public class AggregateByKeyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values,
                          Context context) throws IOException, InterruptedException {

        Integer out = 0;
        for (IntWritable value : values) {
            out += value.get();
        }
        context.write(key, new IntWritable(out));
    }
}
