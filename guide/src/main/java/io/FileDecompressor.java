package io;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.Objects;

/**
 * 根据文件拓展名选取codec解压缩文件
 *
 * @author dengguoqing
 * @date 2018/11/27
 */
public class FileDecompressor {
    public static void main(String[] args) throws IOException {
        String uri = args[0];
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(uri), conf);
        Path path = new Path(uri);

        CompressionCodecFactory factory = new CompressionCodecFactory(conf);
        CompressionCodec codec = factory.getCodec(path);
        if (Objects.isNull(codec)) {
            System.err.println("No Code Found for " + uri);
            System.exit(1);
        }

        String outputUri = CompressionCodecFactory.removeSuffix(uri,
                                                                codec.getDefaultExtension());

        try (InputStream in = codec.createInputStream(fs.open(path));
             OutputStream out = fs.create(new Path(outputUri))) {
            IOUtils.copyBytes(in, out, conf);
        }

        conf.setBoolean(Job.MAP_OUTPUT_COMPRESS,true);


    }
}
