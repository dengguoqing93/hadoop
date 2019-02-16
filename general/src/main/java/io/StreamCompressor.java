package io;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * 该程序压缩从标准输入读取的数据，然后将其写到标准输出
 *
 * @author dengguoqing
 * @date 2019/2/16
 */
public class StreamCompressor {

    public static void main(String[] args) throws Exception{
        String codecClassname = args[0];
        Class<?> codecClass = Class.forName(codecClassname);
        Configuration conf = new Configuration();
        CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, conf);

        CompressionOutputStream out = codec.createOutputStream(System.out);

        IOUtils.copyBytes(System.in, out, 4096, false);
        out.finish();

    }
}
