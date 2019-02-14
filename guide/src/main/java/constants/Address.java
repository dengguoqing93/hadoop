package constants;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

/**
 * ${DESCRIPTION}
 *
 * @author dengguoqing
 * @date 2018/11/26
 */
public class Address {

    private Address() {
    }

    public static final String JRX = "hdfs://master:9000/user/dengguoqing";

    public static final Configuration conf = new Configuration();

}
