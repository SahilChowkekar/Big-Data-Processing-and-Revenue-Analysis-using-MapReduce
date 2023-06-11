import java.io.IOException;

import javax.security.auth.login.Configuration;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.commons.io.FileUtils;
import java.io.File;
import org.apache.hadoop.fs.FileSystem;

public class Sorting{
    public static class SortMapper extends Mapper<Object, Text, FloatWritable, Text> {
        private FloatWritable sorting = new FloatWritable();
        public void map(Object key, Text value, Context context)
        throws IOException, InterruptedException
        {
            String line = value.toString();
            // Replace any consecutive whitespace characters with commas
            line = line.replaceAll("\\s+", "," );
            // Split the line into an array of parts
            String[] part = line.split(",");
            // Parse the third part as a float and multiply it by -1 to sort in descending order
            float sort = (Float.parseFloat(part[2])*-1);
            sorting.set(sort);
             // Write the sorting value as the key and the original value as the output value
            context.write(sorting, value);
        }
    }

    public static class SortReducer extends Reducer<FloatWritable, Text, Text, NullWritable> {
        public void reduce(FloatWritable key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException
        {
            // Iterate over the values associated with the current key
            for (Text value : values) {
                 // Write the value to the output along with a NullWritable
                context.write(value, NullWritable.get());
            }
        }
    }

    public static void main(String[] args) throws Exception
    {        
        Configuration conf = new Configuration();
        Path inputPath = new Path(args[0]);
        Path partitionFile = new Path(args[1] + "_partitions.lst");
        Path outputStage = new Path(args[1] + "_staging");
        Path outputOrder = new Path(args[1]);

        double sampling_rate = 0.001;

        FileSystem.get(new Configuration()).delete(partitionFile, true);
        FileSystem.get(new Configuration()).delete(outputStage, true);
        FileSystem.get(new Configuration()).delete(outputOrder, true);

        Job sampleJob = new Job(conf, "Sampling Phase");
        sampleJob.setJarByClass(Sorting.class);

        sampleJob.setMapperClass(SortMapper.class);
        sampleJob.setNumReduceTasks(0);

        sampleJob.setOutputKeyClass(FloatWritable.class);
        sampleJob.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(sampleJob, inputPath);

        sampleJob.setOutputFormatClass(SequenceFileOutputFormat.class);
        SequenceFileOutputFormat.setOutputPath(sampleJob, outputStage);

        int code = sampleJob.waitForCompletion(true) ? 0 : 1;
        if(code == 0){
            Job orderJob = new Job(conf, "Ordering Phase");
            orderJob.setJarByClass(Sorting.class);
            orderJob.setMapperClass(Mapper.class);
            orderJob.setReducerClass(SortReducer.class);
            orderJob.setNumReduceTasks(1);
            orderJob.setPartitionerClass(TotalOrderPartitioner.class);
            TotalOrderPartitioner.setPartitionFile(orderJob.getConfiguration(), partitionFile);

            orderJob.setOutputKeyClass(FloatWritable.class);
            orderJob.setOutputValueClass(Text.class);
            orderJob.setInputFormatClass(SequenceFileInputFormat.class);
            SequenceFileInputFormat.setInputPaths(orderJob, outputStage);

            TextOutputFormat.setOutputPath(orderJob, outputOrder);
            orderJob.getConfiguration().set("mapreduce.textoutputformat.separator", "");
            InputSampler.writePartitionFile(orderJob, new InputSampler.RandomSampler(.001, 10000, 1));
            code = orderJob.waitForCompletion(true) ? 0 : 2;
        }
        FileSystem.get(new Configuration()).delete(partitionFile, false);
        FileSystem.get(new Configuration()).delete(outputStage, true);
        System.exit(code);
    }
}