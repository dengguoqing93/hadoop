package org.guoqing.map;

import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

/**
 * 通过URLStreamHandler实例以标准输出方式显示Hadoop文件系统的文件
 *
 *
 * @author dengguoqing
 * @date 2019/2/14
 */
public class URLCat {

    static {
        URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
    }

    public static void main(String[] args) throws IOException {
        try (InputStream in = new URL(args[0]).openStream()) {
            IOUtils.copyBytes(in,System.out,4096,false);
        }
    }

}