package compoent;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * mapperReduce公共驱动方法
 *
 * @author dengguoqing
 * @date 2019/2/19
 */
public interface MapperReducerDriver {
    /**
     * 配置输入输出路径
     *
     * @param args 路径参数
     */
    default void configPath(String[] args, Job job) throws IOException {
        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);
        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);
    }
}
