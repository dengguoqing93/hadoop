package applicationDevelop;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

/**
 * ${DESCRIPTION}
 *
 * @author dengguoqing
 * @date 2018/11/28
 */
public class Config {

    @Test
    public void testConfigRead() throws Exception {
        Configuration conf = new Configuration();
        conf.addResource("configuration-1.xml");
        System.out.println(conf.get("size"));
        assertThat(conf.get("color"), is("yellow"));
    }
}
