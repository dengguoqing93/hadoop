package job;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * 购物篮分析映射器
 *
 * @author dengguoqing
 * @date 2019/3/1
 */
public class MBAMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    public static final int DEFAULT_NUMBER_OF_PAIRS = 2;

    private static final Text reduceKey = new Text();

    private static final IntWritable NUMBER_ONE = new IntWritable(1);

    int numberOfPairs;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        numberOfPairs = context.getConfiguration().getInt("number.of.pairs",
                                                          DEFAULT_NUMBER_OF_PAIRS);
    }

    @Override
    protected void map(LongWritable key, Text value,
                       Context context) throws IOException, InterruptedException {
        String line = value.toString().trim();
        List<String> items = convertItemsToList(line);
        if (items != null && !items.isEmpty()) {
            generateMapperOutput(numberOfPairs, items, context);
        }

    }

    /**
     * 通过对输入列表排序建立一个键-值对集合。键是交易中的一个是商品组合，值=1.
     * 这里需要排序以确保（a,b)和（b,a)表示相同的键
     *
     * @param numberOfPairs 关联商品数
     * @param items         商品列表
     * @param context       hadoop作业上下文
     */
    private static void generateMapperOutput(int numberOfPairs, List<String> items,
                                             Context context) throws IOException, InterruptedException {
        List<List<String>> sortedCombinations = Combination.findSortedCombinations(items,
                                                                                   numberOfPairs);
        for (List<String> itemList : sortedCombinations) {
            reduceKey.set(itemList.toString());
            context.write(reduceKey, NUMBER_ONE);
        }
    }

    /**
     * 将字符串转为list
     *
     * @param line 字符串对象
     * @return list
     */
    private static List<String> convertItemsToList(String line) {
        if (StringUtils.isBlank(line)) {
            return null;
        }
        String[] tokens = StringUtils.split(line, ",");
        if (StringUtils.isAllBlank(tokens)) {
            return null;
        }
        List<String> items = new ArrayList<>();
        for (String token : tokens) {
            if (token != null) {
                items.add(token);
            }
        }
        return items;
    }
}
