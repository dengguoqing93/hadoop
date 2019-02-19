package topjob;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * 查找top N猫列表的映射器结构
 *
 * @author dengguoqing
 * @date 2019/2/19
 */
public class TopNMapper extends Mapper<LongWritable, Text, NullWritable, Text> {

    private SortedMap<Double, Text> top10cats = new TreeMap<>();
    private int N = 10;

    /**
     * 对应每个映射器执行一次setup（）函数
     * 这里建立"top N猫列表"
     *
     * @param context 上下文
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        N = conf.getInt("top.n", 10);
    }

    @Override
    protected void map(LongWritable key, Text value,
                       Context context) throws IOException, InterruptedException {
        String[] tokens = value.toString().split(",");
        Double weight = Double.parseDouble(tokens[0]);

        top10cats.put(weight, value);
        if (top10cats.size() > N) {
            top10cats.remove(top10cats.firstKey());
        }
        for (Text text : top10cats.values()) {
            context.write(NullWritable.get(), text);
        }
    }

}
