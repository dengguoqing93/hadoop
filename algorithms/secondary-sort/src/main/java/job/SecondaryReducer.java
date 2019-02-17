package job;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * reduce函数
 *
 * @author dengguoqing
 * @date 2019/2/16
 */
public class SecondaryReducer extends Reducer<DateTemperaturePair, Text, Text, Text> {
    @Override
    protected void reduce(DateTemperaturePair key, Iterable<Text> values,
                          Context context)
            throws IOException, InterruptedException {
        StringBuilder builder = new StringBuilder();
        for (Text value : values) {
            builder.append(value.toString());
            builder.append(",");
        }
        context.write(key.getYearMonth(), new Text(builder.toString()));
    }
}
