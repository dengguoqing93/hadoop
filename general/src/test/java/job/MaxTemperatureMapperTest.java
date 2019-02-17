package job;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.junit.Test;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * 测试mapper
 *
 * @author dengguoqing
 * @date 2019/2/17
 */
public class MaxTemperatureMapperTest {

    @Test
    public void map() throws Exception{
        Mapper.Context context = mock(MaxTemperatureMapper.Context.class);

        MaxTemperatureMapper mapper = new MaxTemperatureMapper();
        mapper.map(null, new Text(
                "0067011990999991950051507004+68750+023550FM-12+038299999V0203301N00671220001CN9999999N9+00001+99999999999"),
                   context);

        verify(context).write(new Text("1950"),new IntWritable(0));
    }
}