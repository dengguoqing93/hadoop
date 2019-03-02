package step1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.junit.Test;
import org.junit.Before;

import java.util.Arrays;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * MaxTemperatureReducer Tester.
 *
 * @author <Authors name>
 * @version 1.0
 * @since <pre>Feb 17, 2019</pre>
 */
public class MaxTemperatureReducerTest {

    @Before
    public void before() throws Exception {
    }


    /**
     * Method: reduce(Text key, Iterable<IntWritable> values, Context context)
     */
    @Test
    public void testReduce() throws Exception {
        MaxTemperatureReducer reducer = new MaxTemperatureReducer();
        Reducer.Context context = mock(MaxTemperatureReducer.Context.class);

        Text key = new Text("1950");
        List<IntWritable> values = Arrays.asList(new IntWritable(10),
                                                 new IntWritable(5));
        reducer.reduce(key, values, context);
        verify(context).write(new Text("1950"), new IntWritable(10));

    }


} 
