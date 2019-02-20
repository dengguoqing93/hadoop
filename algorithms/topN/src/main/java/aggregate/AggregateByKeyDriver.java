package aggregate;

import compoent.MapperReducerDriver;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import utils.ArraysUtils;

/**
 * 驱动器实现
 *
 * @author dengguoqing
 * @date 2019/2/20
 */
public class AggregateByKeyDriver extends Configured implements Tool,
        MapperReducerDriver {
    @Override
    public int run(String[] strings) throws Exception {

        Job job = Job.getInstance();
        job.setJarByClass(getClass());
        job.setJobName("Aggregate");

        FileInputFormat.addInputPath(job, new Path(strings[0]));
        FileOutputFormat.setOutputPath(job, new Path(strings[1]));

        job.setMapperClass(AggregateByKeyMapper.class);
        job.setReducerClass(AggregateByKeyReducer.class);
        job.setCombinerClass(AggregateByKeyReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        return 0;
    }

    public static void main(String[] args) throws Exception{
        ArraysUtils.isEmpty(args);
        System.exit(ToolRunner.run(new AggregateByKeyDriver(), args));
    }
}
