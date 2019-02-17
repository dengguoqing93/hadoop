package developMR;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;
import org.junit.Before;
import org.junit.After;
import org.mockito.Mock;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

/**
 * Develop Tester.
 *
 * @author <Authors name>
 * @version 1.0
 * @since <pre>Feb 17, 2019</pre>
 */
public class DevelopTest {
    

    @Before
    public void before() throws Exception {
    }

    @Test
    public void config() {
        Configuration conf = new Configuration();
        conf.addResource("configuration-1.xml");
        assertThat(conf.get("color"), is("yellow"));
        assertThat(conf.getInt("size", 0), is(10));
        assertThat(conf.get("breadth", "wide"), is("wide"));
    }


} 
