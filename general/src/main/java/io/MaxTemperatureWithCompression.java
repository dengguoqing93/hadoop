package io;

import job.MaxTemperature;
import job.MaxTemperatureMapper;
import job.MaxTemperatureReducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 对查找最高气温作业锁产生的输出进行压缩
 *
 * @author dengguoqing
 * @date 2019/2/16
 */
public class MaxTemperatureWithCompression {
    public static void main(String[] args) throws Exception{
        if (args.length!=2){
            System.err.println(
                    "Usage: MaxTemperatureWithCompression <input path> <output path>");
            System.exit(1);
        }

        Job job = new Job();
        job.setJarByClass(MaxTemperature.class);
        FileInputFormat.addInputPath(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        /*
         *  压缩输出文件
         */
        FileOutputFormat.setCompressOutput(job, true);
        FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);

        job.setMapperClass(MaxTemperatureMapper.class);
        job.setCombinerClass(MaxTemperatureReducer.class);
        job.setReducerClass(MaxTemperatureReducer.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
