package io;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;

import java.net.URI;

/**
 * ${DESCRIPTION}
 *
 * @author dengguoqing
 * @date 2018/11/28
 */
public class SequenceFileReadDemo {
    public static void main(String[] args) throws Exception {
        String uri = "number.seq";
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(uri), conf);
        Path path = new Path(uri);

        try (SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, conf)) {
            Writable key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(),
                                                                  conf);
            Writable value = (Writable) ReflectionUtils.newInstance(
                    reader.getValueClass(), conf);

            long position = reader.getPosition();
            while (reader.next(key, value)) {
                String sysncSeen = reader.syncSeen() ? "*" : "";
                System.out.printf("[%s%s]\t%s\t%s\n", position, sysncSeen, key, value);
                position = reader.getPosition();
            }
        }
    }
}
