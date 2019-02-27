package job;

import average.TimeSeriesData;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * 移动平均的归约器
 *
 * @author dengguoqing
 * @date 2019/2/27
 */
public class SortInMemory_MovingAverageReducer extends
        Reducer<Text, TimeSeriesData, Text, Text> {
    private int windowSize = 4;

    private static final Logger LOGGER = LoggerFactory.getLogger(
            SortInMemory_MovingAverageReducer.class);

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        windowSize = context.getConfiguration().getInt("moving.average.window.size", 4);
    }

    @Override
    protected void reduce(Text key, Iterable<TimeSeriesData> values,
                          Context context) throws IOException, InterruptedException {
        LOGGER.info("reduce(): key=" + key.toString());
        List<TimeSeriesData> timeseries = new ArrayList<>();
        for (TimeSeriesData tsData : values) {
            TimeSeriesData copy = TimeSeriesData.copy(tsData);
            timeseries.add(copy);
        }

        // sort the timeseries data in memory and
        // apply moving average algorithm to sorted timeseries
        Collections.sort(timeseries);
        LOGGER.info("reduce(): timeseries=" + timeseries.toString());


        // calculate prefix sum
        double sum = 0.0;
        for (int i = 0; i < windowSize - 1; i++) {
            sum += timeseries.get(i).getValue();
        }

        // now we have enough timeseries data to calculate moving average
        Text outputValue = new Text();
        for (int i = windowSize - 1; i < timeseries.size(); i++) {
            LOGGER.info("reduce(): key=" + key.toString() + "  i=" + i);
            sum += timeseries.get(i).getValue();
            double movingAverage = sum / windowSize;
            long timestamp = timeseries.get(i).getTimestamp();
            outputValue.set(LocalDate.ofEpochDay(timestamp) + "," + movingAverage);
            // send output to HDFS
            context.write(key, outputValue);

            // prepare for next iteration
            sum -= timeseries.get(i - windowSize + 1).getValue();
        }
    }
}
