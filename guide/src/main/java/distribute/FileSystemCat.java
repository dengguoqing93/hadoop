package distribute;

import constants.Address;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

/**
 * ${DESCRIPTION}
 *
 * @author dengguoqing
 * @date 2018/11/23
 */
public class FileSystemCat {
    public static void main(String[] args) throws IOException {
        String uri = Address.JRX + "/hadoop.txt";
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", uri);
        FileSystem fs = FileSystem.get(URI.create(uri), configuration);
        try (InputStream inputStream = fs.open(new Path(uri))) {
            IOUtils.copyBytes(inputStream, System.out, 4096, false);
        }
    }
}
