package step2;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import sun.security.provider.ConfigFile;

import java.io.IOException;

/**
 * 定义map()完成地址统计
 *
 * @author dengguoqing
 * @date 2019/2/26
 */
public class LocationCountMapper extends Mapper<Text, Text, Text, Text> {
    @Override
    protected void map(Text key, Text value,
                       Context context) throws IOException, InterruptedException {
        context.write(key, value);
        /*String[] split = StringUtils.split(value.toString(), "\t");

        context.write(new Text(split[0]), new Text(split[1]));*/


    }
}
