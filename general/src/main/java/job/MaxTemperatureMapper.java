package job;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 最高气温MAPPER
 *
 * @author dengguoqing
 * @date 2019/2/16
 */
public class MaxTemperatureMapper
        extends Mapper<LongWritable, Text, Text, IntWritable> {

    private NcdcRecordParser parser = new NcdcRecordParser();

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        parser.parse(value);

        if (parser.isValidTemperature()) {
            context.write(new Text(parser.getYear()),
                          new IntWritable(parser.getAirTemperature()));
        }
    }
}
