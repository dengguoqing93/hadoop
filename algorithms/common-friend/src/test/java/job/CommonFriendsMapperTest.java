package job;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.junit.Test;
import org.mockito.Mockito;

import static org.mockito.ArgumentMatchers.any;

/**
 * 共同好友映射器
 *
 * @author dengguoqing
 * @date 2019/3/2
 */
public class CommonFriendsMapperTest {

    @Test
    public void map() throws Exception {
        CommonFriendsMapper mapper = new CommonFriendsMapper();

        Mapper<org.apache.hadoop.io.LongWritable, org.apache.hadoop.io.Text, org.apache.hadoop.io.Text, org.apache.hadoop.io.Text>.Context context = Mockito.mock(
                CommonFriendsMapper.Context.class);

        LongWritable key = new LongWritable(1);
        Text text = new Text("100 200 300 400");
        Text outKey = new Text("100,400");
        Text outValue = new Text("200,300,400");
        mapper.map(key, text, context);
        Mockito.verify(context,Mockito.times(3)).write(any(), any());
    }
}