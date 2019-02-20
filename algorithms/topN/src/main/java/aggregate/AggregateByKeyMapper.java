package aggregate;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 聚合映射
 *
 * @author dengguoqing
 * @date 2019/2/20
 */
public class AggregateByKeyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    @Override
    protected void map(LongWritable key, Text value,
                       Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split(",");
        Text outputKey = new Text(split[0]);
        IntWritable outputValue = new IntWritable(Integer.valueOf(split[1]));
        context.write(outputKey, outputValue);

    }
}
