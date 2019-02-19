package topjob;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * TopN算法归约器
 *
 * @author dengguoqing
 * @date 2019/2/19
 */
public class TopNReducer extends Reducer<NullWritable, Text, Text, Text> {

    private int N = 10;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration configuration = context.getConfiguration();
        N = configuration.getInt("top.n", 10);

    }

    @Override
    protected void reduce(NullWritable key, Iterable<Text> values,
                          Context context) throws IOException, InterruptedException {
        SortedMap<Double, Text> finaltop10 = new TreeMap<>();
        for (Text value : values) {
            String[] tokens = value.toString().split(",");
            Double weight = Double.parseDouble(tokens[0]);
            finaltop10.put(weight, value);
            if (finaltop10.size() > N) {
                finaltop10.remove(finaltop10.firstKey());
            }
        }

        for (Map.Entry<Double, Text> doubleTextEntry : finaltop10.entrySet()) {
            context.write(new Text(doubleTextEntry.getKey().toString()),
                          doubleTextEntry.getValue());
        }
    }
}
