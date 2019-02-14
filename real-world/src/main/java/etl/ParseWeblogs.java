package etl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * ${DESCRIPTION}
 *
 * @author dengguoqing
 * @date 2019/1/27
 */
public class ParseWeblogs extends Configured implements Tool {
    @Override
    public int run(String[] strings) throws Exception {
        Path inputPath = new Path(strings[0]);
        Path outputPath = new Path(strings[1]);
        Configuration conf = getConf();
        Job weblogJob = new Job(conf);
        weblogJob.setJobName("Weblog Transformer");
        weblogJob.setJarByClass(getClass());
        weblogJob.setNumReduceTasks(0);
        weblogJob.setMapperClass(CLFMapper.class);
        weblogJob.setMapOutputValueClass(Text.class);
        weblogJob.setMapOutputKeyClass(Text.class);
        weblogJob.setOutputKeyClass(Text.class);
        weblogJob.setOutputValueClass(Text.class);
        weblogJob.setOutputFormatClass(TextOutputFormat.class);
        weblogJob.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.setInputPaths(weblogJob, inputPath);
        FileOutputFormat.setOutputPath(weblogJob, outputPath);
        return weblogJob.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int returnCode = ToolRunner.run(new ParseWeblogs(), args);
        System.exit(returnCode);
    }
}
