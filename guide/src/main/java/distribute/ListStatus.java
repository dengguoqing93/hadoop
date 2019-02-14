package distribute;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;

import static constants.Address.JRX;
import static constants.Address.conf;

/**
 * ${DESCRIPTION}
 *
 * @author dengguoqing
 * @date 2018/11/26
 */
public class ListStatus {
    public static void main(String[] args) throws IOException {
        String uri = JRX;
        FileSystem fs = FileSystem.get(URI.create(uri), conf);
        Path path = new Path(JRX);
        FileStatus[] fileStatuses = fs.listStatus(path);
        Path[] paths = FileUtil.stat2Paths(fileStatuses);
        for (Path path1 : paths) {
            System.out.println(path1);
        }

    }
}
