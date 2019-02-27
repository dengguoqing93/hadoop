package step1;

import edu.umd.cloud9.io.pair.PairOfStrings;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;
import static org.mockito.Mockito.verify;

/**
 * ${DESCRIPTION}
 *
 * @author dengguoqing
 * @date 2019/2/26
 */
public class LeftJoinReducerTest {

    @Test
    public void reduce() throws IOException, InterruptedException {
        LeftJoinReducer reducer = new LeftJoinReducer();
        Reducer.Context context = Mockito.mock(LeftJoinReducer.Context.class);
        PairOfStrings pairOfStrings = new PairOfStrings();
        PairOfStrings pairOfStrings1 = new PairOfStrings();
        pairOfStrings.set("L", "u1");
        pairOfStrings1.set("P", "p1");
        List<PairOfStrings> list = Arrays.asList(pairOfStrings, pairOfStrings1);

        reducer.reduce(pairOfStrings, list, context);
        verify(context).write(new Text("p1"), new Text("p1"));
    }
}