import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import java.io.IOException;
import java.util.Comparator;
import java.util.Map;
import java.util.TreeMap;

import javax.security.auth.login.Configuration;

import java.util.List;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

public class Max {
    public static class MaxMapper extends Mapper<LongWritable, Text, Text, Text> {
        private Text outputKey = new Text();
        private Text outputValue = new Text();

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            line = line.replaceAll("\\s+", ",");
            // Replace any consecutive whitespace characters with a comma
            String[] parts = line.split(",");

            String year = parts[0];
            String month = parts[1];
            float price = Float.parseFloat(parts[2]);
            // Concatenate month and price into a single string, separated by a comma
            String v = month + "," + Float.toString(price);

            // Set the year as the output key
            outputKey.set(year);
            // Set the concatenated string of month and price as the output value
            outputValue.set(v);
            // Emit the key-value pair to the output
            context.write(outputKey, outputValue);

        }
    }

    public static class MaxReducer extends Reducer<Text, Text, Text, Text> {
        private Text result = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            float maxRevenue = 0;
            String month = "";
            for (Text value : values) {
                String k = value.toString();
                // Split the concatenated value into month and revenue parts
                String[] parts = k.split(",");

                // Check if the revenue is greater than the current maxRevenue
                if (Float.parseFloat(parts[1]) > maxRevenue) {
                    maxRevenue = Float.parseFloat(parts[1]);
                    month = parts[0];
                    // Update the maxRevenue and month if a new maximum is found
                }
            }
            // Concatenate the month and maxRevenue with a space in between
            String max = month + " " + Float.toString(maxRevenue);
            // Set the result as the concatenated string
            result.set(max);
            // Write the key-value pair to the output
            context.write(key, result);

        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        @SuppressWarnings("deprecation")
        Job job = new Job(conf, "Max_dataset");
        job.setJarByClass(Max.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(MaxMapper.class);
        job.setReducerClass(MaxReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        Path out = new Path(args[1]);
        out.getFileSystem(conf).delete(out);
        job.waitForCompletion(true);
    }
}