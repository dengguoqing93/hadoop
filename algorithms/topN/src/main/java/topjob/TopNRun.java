package topjob;

import compoent.MapperReducerDriver;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 本机运行代码
 *
 * @author dengguoqing
 * @date 2019/2/19
 */
public class TopNRun{
    public static void main(
            String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        conf.set("top.n", "5");
        Job job = Job.getInstance(conf);
        job.setJobName("Top 10 List");
        job.setJarByClass(TopNRun.class);


        FileInputFormat.addInputPath(job, new Path(
                "file:///Users/dengguoqing/IdeaProjects/hadoop/algorithms/topN/src/main/resources/sample_input.txt"));
        FileOutputFormat.setOutputPath(job, new Path(
                "file:///Users/dengguoqing/IdeaProjects/hadoop/algorithms/topN/src/main/resources/output"));

        job.setMapperClass(TopNMapper.class);
        job.setReducerClass(TopNReducer.class);

        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.waitForCompletion(true);
    }
}
