package topjob;

import compoent.MapperReducerDriver;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 提交作业的驱动器
 *
 * @author dengguoqing
 * @date 2019/2/19
 */
public class TopNDriver extends Configured implements Tool, MapperReducerDriver {

    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = getConf();

        if (strings.length == 3) {
            conf.set("top.n", strings[2]);
        }
        Job job = Job.getInstance(conf);
        job.setJobName("Top N");
        job.setJarByClass(getClass());

        configPath(strings, job);

        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setMapperClass(TopNMapper.class);
        job.setReducerClass(TopNReducer.class);

        return job.waitForCompletion(true) ? 0 : 1;

    }

    public static void main(String[] args) throws Exception {
        if (null == args || args.length == 0) {
            System.err.println("错误的参数");
        }
        int run = ToolRunner.run(new TopNDriver(), args);
        System.exit(run);
    }
}
