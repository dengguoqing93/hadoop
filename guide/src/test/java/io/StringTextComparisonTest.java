package io;

import org.apache.hadoop.io.Text;
import org.junit.Test;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

/**
 * ${DESCRIPTION}
 *
 * @author dengguoqing
 * @date 2018/11/27
 */
public class StringTextComparisonTest {

    @Test
    public void string() throws Exception {
        String s = "这是个测试";
        assertThat(s.length(), is(5));
        assertThat(s.getBytes("UTF-8").length, is(15));
    }

    @Test
    public void text()throws Exception{
        Text text = new Text("这是个测试");
        assertThat(text.getLength(), is(15));
        assertThat(text.getBytes().length, is(23));
        System.out.println(text.charAt(3));
    }
}
