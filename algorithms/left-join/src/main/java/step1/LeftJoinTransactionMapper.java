package step1;

import edu.umd.cloud9.io.pair.PairOfStrings;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 左连接交易映射器
 *
 * @author dengguoqing
 * @date 2019/2/26
 */
public class LeftJoinTransactionMapper extends
        Mapper<LongWritable, Text, PairOfStrings, PairOfStrings> {

    PairOfStrings outKey = new PairOfStrings();
    PairOfStrings outValue = new PairOfStrings();

    @Override
    protected void map(LongWritable key, Text value,
                       Context context) throws IOException, InterruptedException {
        String input = value.toString();
        String[] tokens = StringUtils.split(input, ",");
        String productId = tokens[1];
        String userId = tokens[2];

        outKey.set(userId, "2");
        outValue.set("P", productId);
        context.write(outKey, outValue);
    }
}
