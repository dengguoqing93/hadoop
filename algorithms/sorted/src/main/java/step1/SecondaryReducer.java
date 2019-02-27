package step1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.time.LocalDate;

/**
 * reduce函数
 *
 * @author dengguoqing
 * @date 2019/2/16
 */
public class SecondaryReducer extends Reducer<CompositeKey, NaturalValue, Text, Text> {


    @Override
    public void reduce(CompositeKey key,
                       Iterable<NaturalValue> values,
                       Context context)
            throws IOException, InterruptedException {

        // note that values are sorted.
        // apply moving average algorithm to sorted timeseries
        StringBuilder builder = new StringBuilder();
        for (NaturalValue data : values) {
            builder.append("(");
            String dateAsString = LocalDate.ofEpochDay(key.getTimestamp()).toString();
            double price = data.getPrice();
            builder.append(dateAsString);
            builder.append(",");
            builder.append(price);
            builder.append(")");
        }
        context.write(new Text(key.getStockSymbol()), new Text(builder.toString()));
    } // reduce
}
