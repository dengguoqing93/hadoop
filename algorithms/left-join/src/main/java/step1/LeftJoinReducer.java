package step1;

import edu.umd.cloud9.io.pair.PairOfStrings;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * 左连接归约器
 *
 * @author dengguoqing
 * @date 2019/2/26
 */
public class LeftJoinReducer extends Reducer<PairOfStrings, PairOfStrings, Text, Text> {


    Text productID = new Text();
    Text locationID = new Text("undefined");

    @Override
    protected void reduce(PairOfStrings key, Iterable<PairOfStrings> values,
                          Context context) throws IOException, InterruptedException {
        Iterator<PairOfStrings> iterator = values.iterator();
        if (iterator.hasNext()) {
            PairOfStrings firstPair = iterator.next();
            System.err.println("firstPair="+firstPair.toString());
            if (firstPair.getLeftElement().equals("L")) {
                locationID.set(firstPair.getValue());
            }
        }

        while (iterator.hasNext()) {
            PairOfStrings productPair = iterator.next();
            System.err.println("productPair="+productPair.toString());
            productID.set(productPair.getRightElement());
            context.write(productID, locationID);
        }
    }
}
