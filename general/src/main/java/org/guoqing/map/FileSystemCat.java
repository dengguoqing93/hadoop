package org.guoqing.map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

/**
 * 直接使用FileSystem以标准输出格式显示Hadoop文件系统中的文件
 *
 * @author dengguoqing
 * @date 2019/2/14
 */
public class FileSystemCat {
    public static void main(String[] args) throws IOException {
        String uri;
        if (0 == args.length){
            uri = "hdfs://master:9000/user/dengguoqing/quangle.txt";
        }else {
            uri = args[0];
        }
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(uri), conf);
        try (FSDataInputStream in = fs.open(new Path(uri))) {
            IOUtils.copyBytes(in, System.out, 4096, false);
            in.seek(0);
            IOUtils.copyBytes(in, System.out, 4096, false);
        }
    }
}
