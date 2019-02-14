package org.guoqing.map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.*;
import java.net.URI;

/**
 * 将本地文件复制到Hadoop文件系统
 *
 * @author dengguoqing
 * @date 2019/2/14
 */
public class FileCopyWithProgress {
    public static void main(String[] args) throws Exception {
        String localSrc = args[0];
        String dst = args[1];
        try(InputStream in = new BufferedInputStream(new FileInputStream(localSrc))){
            Configuration conf = new Configuration();
            FileSystem fs = FileSystem.get(URI.create(dst), conf);
            OutputStream out = fs.create(new Path(dst), () -> System.out.println("."));
            IOUtils.copyBytes(in,out,4096,true);
        }
    }
}
