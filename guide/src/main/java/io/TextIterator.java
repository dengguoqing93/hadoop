package io;

import org.apache.hadoop.io.Text;

import java.nio.ByteBuffer;

/**
 * ${DESCRIPTION}
 *
 * @author dengguoqing
 * @date 2018/11/27
 */
public class TextIterator {
    public static void main(String[] args) throws Exception {

        Text text = new Text("这也是测试");
        ByteBuffer buffer = ByteBuffer.wrap(text.getBytes(), 0, text.getLength());

        int cp;

        while (buffer.hasRemaining() && (cp = Text.bytesToCodePoint(buffer)) != -1) {
            System.out.println(Integer.toHexString(cp));
        }
    }
}
