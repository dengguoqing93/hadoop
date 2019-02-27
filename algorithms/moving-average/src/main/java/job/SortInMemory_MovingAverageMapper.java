package job;

import average.TimeSeriesData;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.LocalDate;

/**
 * 移动平均的映射器函数
 *
 * @author dengguoqing
 * @date 2019/2/27
 */
public class SortInMemory_MovingAverageMapper extends
        Mapper<LongWritable, Text, Text, TimeSeriesData> {

    private static final Logger LOGGER = LoggerFactory.getLogger(
            SortInMemory_MovingAverageMapper.class);

    @Override
    protected void map(LongWritable key, Text value,
                       Context context) throws IOException, InterruptedException {
        if ((value == null) || (value.getLength() == 0)) {
            return;
        }
        String[] tokens = StringUtils.split(value.toString(), ",");
        if (tokens.length == 3) {
            Text reducerKey = new Text(tokens[0]);
            long timestamp = LocalDate.parse(tokens[1]).toEpochDay();
            double price = Double.parseDouble(tokens[2]);
            TimeSeriesData timeSeriesData = new TimeSeriesData(timestamp, price);
            context.write(reducerKey, timeSeriesData);
        }else {
            LOGGER.error("not enough tokens");
        }
    }
}
