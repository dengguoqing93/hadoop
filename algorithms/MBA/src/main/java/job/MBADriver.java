package job;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;



/**
 * 购物篮分析驱动器
 *
 * @author dengguoqing
 * @date 2019/3/1
 */
public class MBADriver {
    public static void main(String[] args) throws Exception {
        String inputPath;
        String outputPath;
        if (args == null || args.length == 0) {
            inputPath = "file:///Users/dengguoqing/IdeaProjects/hadoop/algorithms/MBA/src/main/resources/input.txt";
            outputPath = "file:///Users/dengguoqing/IdeaProjects/hadoop/algorithms/MBA/src/main/resources/output";
        } else {
            inputPath = args[0];
            outputPath = args[1];
        }
        Job job = Job.getInstance();
        job.setJobName("购物篮分析");
        job.setJarByClass(MBADriver.class);

        FileInputFormat.setInputPaths(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        //输出格式
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(MBAMapper.class);
        job.setCombinerClass(MBAReducer.class);
        job.setReducerClass(MBAReducer.class);

        job.waitForCompletion(true);

    }
}
