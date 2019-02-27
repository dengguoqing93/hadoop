package step1;

import edu.umd.cloud9.io.pair.PairOfStrings;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;

import static org.junit.Assert.*;

/**
 * ${DESCRIPTION}
 *
 * @author dengguoqing
 * @date 2019/2/26
 */
public class LeftJoinTransactionMapperTest {

    @Test
    public void map() throws IOException, InterruptedException {
        LeftJoinTransactionMapper mapper = new LeftJoinTransactionMapper();
        Mapper.Context mock = Mockito.mock(Mapper.Context.class);
        LongWritable longWritable = new LongWritable(1);
        Text text = new Text("t1,p2,u1,3,330");
        mapper.map(longWritable, text, mock);
        PairOfStrings value = new PairOfStrings("P", "p2");
        PairOfStrings key = new PairOfStrings("u1", "2");
        Mockito.verify(mock).write(key, value);
    }
}