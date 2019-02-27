package step1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.junit.Test;
import org.junit.Before;

import java.time.LocalDate;
import java.time.temporal.ChronoField;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * SecondaryMapper Tester.
 *
 * @author <Authors name>
 * @version 1.0
 * @since <pre>Feb 18, 2019</pre>
 */
public class SecondaryMapperTest {

    @Before
    public void before() throws Exception {
    }


    /**
     * Method: map(LongWritable key, Text value, Context context)
     */
    @Test
    public void testMap() throws Exception {
        SecondaryMapper.Context context = mock(Mapper.Context.class);
        SecondaryMapper mapper = new SecondaryMapper();
        Text text = new Text("ILMN,2013-12-05,97.65");
        mapper.map(new LongWritable(1), text, context);
        LocalDate date = LocalDate.parse("2013-12-05");
        long timestamp = date.getLong(ChronoField.EPOCH_DAY);

        CompositeKey compositeKey = new CompositeKey("ILMN", timestamp);
        NaturalValue naturalValue = new NaturalValue(timestamp, 97.65);
        verify(context).write(compositeKey, naturalValue);

    }


} 
