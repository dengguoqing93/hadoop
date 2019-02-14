package org.guoqing.map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

import java.util.Arrays;

import static java.net.URI.create;

/**
 * 显示Hadoop文件系统中一组路径的文件信息
 *
 * @author dengguoqing
 * @date 2019/2/14
 */
public class ListStatus {

    public static void main(String[] args) throws Exception {
        String uri = args[0];
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(create(uri), conf);

        Path[] paths = new Path[args.length];

        for (int i = 0; i < paths.length; i++) {
            paths[i] = new Path(args[i]);
        }

        FileStatus[] status = fs.listStatus(paths);

        Path[] listPaths = FileUtil.stat2Paths(status);

        Arrays.stream(listPaths).forEach(t -> System.out.println(t));


    }

}
