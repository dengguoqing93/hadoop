package io;

import org.apache.hadoop.io.*;
import org.junit.Test;
import org.junit.Before;
import org.junit.After;

import java.io.*;


import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

/**
 * Writable1 Tester.
 *
 * @author <Authors name>
 * @version 1.0
 * @since <pre>Nov 27, 2018</pre>
 */
public class Writable1Test {

    @Before
    public void before() throws Exception {
    }

    @After
    public void after() throws Exception {
    }

    @Test
    public void test() throws Exception {
        IntWritable writable = new IntWritable(163);
        byte[] bytes = serialize(writable);
        IntWritable newWritable = new IntWritable();
        deserialize(newWritable, bytes);
        assertThat(newWritable.get(), is(163));
    }

    @Test
    public void testCompare() throws Exception {
        RawComparator<IntWritable> comparator = WritableComparator.get(IntWritable.class);
        IntWritable w1 = new IntWritable(163);
        IntWritable w2 = new IntWritable(64);
        assertThat(comparator.compare(w1, w2), is(1));

        byte[] b1 = serialize(w1);
        byte[] b2 = serialize(w2);
        assertThat(comparator.compare(b1, 0, b1.length, b2, 0, b2.length), is(1));


    }

    @Test
    public void testText() throws Exception {
        Text text = new Text("Hadoop");
        assertThat(text.getLength(), is(6));

        assertThat(text.getBytes().length, is(6));

        assertThat(text.charAt(2), is((int) 'd'));

        assertThat("Out of bounds", text.charAt(45), is(-1));

        assertThat("Find a substring", text.find("do"), is(2));

        assertThat("Find first 'o'", text.find("o"), is(3));

        assertThat("Find 'o' from position 4 or later", text.find("o", 4), is(4));

        assertThat("No Match", text.find("pig"), is(-1));
    }

    public static byte[] serialize(Writable writable) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DataOutputStream dataOut = new DataOutputStream(out);
        writable.write(dataOut);
        dataOut.close();
        return out.toByteArray();
    }

    public static byte[] deserialize(Writable writable, byte[] bytes) throws IOException {
        ByteArrayInputStream in = new ByteArrayInputStream(bytes);
        DataInputStream dataIn = new DataInputStream(in);
        writable.readFields(dataIn);
        dataIn.close();
        return bytes;
    }
} 
