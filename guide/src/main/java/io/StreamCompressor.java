package io;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;

/**
 * 压缩从标准输入读取的数据，然后将其写到标准输出
 *
 * @author dengguoqing
 * @date 2018/11/27
 */
public class StreamCompressor {
    public static void main(String[] args) throws ClassNotFoundException {
        String codecClassName = "org.apache.hadoop.io.compress.GzipCodec";
        if (args!=null){
            codecClassName = args[0];
        }

        Class<?> codecClass = Class.forName(codecClassName);
        Configuration conf = new Configuration();
        CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(
                codecClass, conf);

        try (CompressionOutputStream outputStream = codec.createOutputStream(
                System.out)) {
            IOUtils.copyBytes(System.in, outputStream, 4096, false);
            outputStream.finish();
        } catch (IOException e) {
            e.printStackTrace();
        }


    }
}
