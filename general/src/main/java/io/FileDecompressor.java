package io;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.Objects;

/**
 * 根据文件名选取codec解压缩文件
 *
 * @author dengguoqing
 * @date 2019/2/16
 */
public class FileDecompressor {
    public static void main(String[] args) throws Exception{
        String uri = args[0];
        Configuration conf = new Configuration();
        FileSystem fileSystem = FileSystem.get(URI.create(uri), conf);

        Path inputPath = new Path(uri);
        CompressionCodecFactory factory = new CompressionCodecFactory(conf);
        CompressionCodec codec = factory.getCodec(inputPath);

        if (Objects.isNull(codec)){
            System.err.println("NO codec found for " + uri);
            System.exit(1);
        }

        String outputUri = CompressionCodecFactory.removeSuffix(uri,
                                                                codec.getDefaultExtension());

        try (InputStream inputStream = codec.createInputStream(
                fileSystem.open(inputPath));
             OutputStream out = fileSystem.create(new Path(outputUri))) {
            IOUtils.copyBytes(inputStream, out, conf);
        }

    }
}
