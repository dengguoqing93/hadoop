package step1;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 启动类
 *
 * @author dengguoqing
 * @date 2019/2/16
 */
public class MaxTemperature {

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            args = new String[]{
                 "file:///Users/dengguoqing/IdeaProjects/hadoop/general/src/main/resources/sample.txt",
                    "file:///Users/dengguoqing/IdeaProjects/hadoop/general/src/main/resources/output"
            };
        }

        Job job = Job.getInstance();
        job.setJarByClass(MaxTemperature.class);
        job.setJobName("Max temperature");

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(MaxTemperatureMapper.class);
        job.setReducerClass(MaxTemperatureReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
