package job;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.junit.Test;
import org.junit.Before;
import org.junit.After;

import java.time.LocalDate;
import java.time.temporal.ChronoField;
import java.util.Arrays;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * SecondaryReducer Tester.
 *
 * @author <Authors name>
 * @version 1.0
 * @since <pre>Feb 18, 2019</pre>
 */
public class SecondaryReducerTest {

    @Before
    public void before() throws Exception {
    }


    /**
     * Method: reduce(CompositeKey key, Iterable<NaturalValue> values, Context context)
     */
    @Test
    public void testReduce() throws Exception {

        SecondaryReducer.Context context = mock(Reducer.Context.class);
        SecondaryReducer reducer = new SecondaryReducer();
        LocalDate date = LocalDate.parse("2013-12-05");
        long timestamp = date.getLong(ChronoField.EPOCH_DAY);

        CompositeKey compositeKey = new CompositeKey("ILMN", timestamp);
        NaturalValue naturalValue = new NaturalValue(timestamp, 97.65);

        reducer.reduce(compositeKey, Arrays.asList(naturalValue), context);
        verify(context).write(new Text("ILMN"),new Text("(2013-12-05,97.65)"));
    }


} 
