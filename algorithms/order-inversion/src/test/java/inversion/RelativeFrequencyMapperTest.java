package inversion;

import edu.umd.cloud9.io.pair.PairOfStrings;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;

import static org.junit.Assert.*;
import static org.mockito.Mockito.verify;

/**
 * ${DESCRIPTION}
 *
 * @author dengguoqing
 * @date 2019/2/27
 */
public class RelativeFrequencyMapperTest {

    @Test
    public void map() throws IOException, InterruptedException {
        RelativeFrequencyMapper mapper = new RelativeFrequencyMapper();
        Mapper.Context context = Mockito.mock(RelativeFrequencyMapper.Context.class);
        LongWritable key = new LongWritable(1);
        Text text = new Text("Java is a");
        mapper.map(key, text, context);
        PairOfWords pair = new PairOfWords();
        pair.set("a","*");
        IntWritable intWritable = new IntWritable(1);
        verify(context).write(pair, intWritable);
    }
}