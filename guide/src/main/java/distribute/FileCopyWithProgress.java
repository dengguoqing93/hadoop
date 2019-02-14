package distribute;

/**
 * ${DESCRIPTION}
 *
 * @author dengguoqing
 * @date 2018/11/26
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.URI;

import static constants.Address.*;

public class FileCopyWithProgress {
    public static void main(String[] args) {
        String local = "guide/src/main/resources/test-1.txt";
        String dst = JRX + "/test-1.txt";
        try (InputStream in = new BufferedInputStream(new FileInputStream(local))) {
            Configuration conf = new Configuration();
            FileSystem fs = FileSystem.get(URI.create(dst), conf);
            FSDataOutputStream out = fs.create(new Path(dst),
                                               () -> System.out.println("."));
            IOUtils.copyBytes(in, out, 4096, true);
        } catch (Exception e) {
            System.out.println(e);
        }

    }
}
