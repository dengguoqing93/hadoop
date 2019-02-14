package assist;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Test;
import org.junit.Before;
import org.junit.After;

import java.util.Arrays;

/**
 * MaxTemperatureReducer Tester.
 *
 * @author <Authors name>
 * @version 1.0
 * @since <pre>Nov 29, 2018</pre>
 */
public class MaxTemperatureReducerTest {

    @Before
    public void before() throws Exception {
    }

    @After
    public void after() throws Exception {
    }

    @Test
    public void returnsMaximumIntegerInValues() throws Exception {
        new ReduceDriver<Text, IntWritable, Text, IntWritable>().withReducer(
                new MaxTemperatureReducer())
                .withInput(new Text("1950"),
                           Arrays.asList(new IntWritable(10), new IntWritable(5)))
                .withOutput(new Text("1950"), new IntWritable(10)).runTest();
    }


} 
