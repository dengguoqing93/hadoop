package step1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.time.LocalDate;
import java.time.temporal.ChronoField;

/**
 * mapper类
 *
 * @author dengguoqing
 * @date 2019/2/16
 */
public class SecondaryMapper extends
        Mapper<LongWritable, Text, CompositeKey, NaturalValue> {

    private final CompositeKey outKey = new CompositeKey();

    private final NaturalValue outValue = new NaturalValue();

    @Override
    protected void map(LongWritable key, Text value,
                       Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] tokens = line.split(",");
        String symbol = tokens[0];
        LocalDate date = LocalDate.parse(tokens[1]);
        double price = Double.valueOf(tokens[2]);
        outKey.set(tokens[0], date.getLong(ChronoField.EPOCH_DAY));
        outValue.set(date.getLong(ChronoField.EPOCH_DAY), price);
        context.write(outKey, outValue);
    }
}
