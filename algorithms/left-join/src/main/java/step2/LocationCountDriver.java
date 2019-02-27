package step2;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * 提交阶段2作业的驱动器
 *
 * @author dengguoqing
 * @date 2019/2/26
 */
public class LocationCountDriver {

    public static void main(String[] args) throws Exception {
        Job job = Job.getInstance();
        job.setJarByClass(LocationCountDriver.class);
        job.setJobName("地址统计");

        FileInputFormat.addInputPath(job, new Path(
                "file:///Users/dengguoqing/IdeaProjects/hadoop/algorithms/left-join/src/main/resources/output/part-r-00000"));
        FileOutputFormat.setOutputPath(job, new Path(
                "file:///Users/dengguoqing/IdeaProjects/hadoop/algorithms/left-join/src/main/resources/output1"));

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(LocationCountMapper.class);
        job.setReducerClass(LocationCountReducer.class);

        job.waitForCompletion(true);
    }
}
