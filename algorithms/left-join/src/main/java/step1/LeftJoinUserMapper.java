package step1;

import edu.umd.cloud9.io.pair.PairOfStrings;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 左连接用户映射器
 *
 * @author dengguoqing
 * @date 2019/2/26
 */
public class LeftJoinUserMapper extends
        Mapper<LongWritable, Text, PairOfStrings, PairOfStrings> {

    private PairOfStrings outKey = new PairOfStrings();
    private PairOfStrings outValue = new PairOfStrings();

    @Override
    protected void map(LongWritable key, Text value,
                       Context context) throws IOException, InterruptedException {
        String input = value.toString();
        String[] tokens = StringUtils.split(input, ",");
        String UserId = tokens[0];
        String locationId = tokens[1];
        outKey.set(UserId, "1");
        outValue.set("L", locationId);
        context.write(outKey, outValue);
    }
}
