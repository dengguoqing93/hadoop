package step2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * 定义reduce()完成地址统计
 *
 * @author dengguoqing
 * @date 2019/2/26
 */
public class LocationCountReducer extends Reducer<Text, Text, Text, IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<Text> values,
                          Context context) throws IOException, InterruptedException {
        Set<String> set = new HashSet<>();
        for (Text value : values) {
            set.add(value.toString());
        }
        context.write(key, new IntWritable(set.size()));

    }
}
