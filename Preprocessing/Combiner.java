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
import java.util.List;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;



public class Combiner{
    public static class CombinerMapper extends Mapper<LongWritable, Text, Text, Text> {
		private Text outkey = new Text();
        private Text outvalue = new Text();
        public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException
        {
        String record = value.toString();
        String[] parts = record.split(",");
		
		// Checking dataset length
		if (parts.length==10)
		{
			
				outkey.set(parts[4] + "," + parts[5]);
				// Calculate total revenue 
				float totalRevenue = Float.parseFloat(parts[8]) * (Integer.parseInt(parts[1]) + Integer.parseInt(parts[2]));
				String revenueString = Float.toString(totalRevenue);
				outvalue.set(revenueString);
				outvalue.set(parts[8]);
				context.write(outkey, outvalue);	
			
		}
		else 
		{
			
				// Processing records with other lengths
				String month = parts[4].toLowerCase().trim();
				int monthValue;
				
				if (Character.isDigit(month.charAt(0))) {
					 // If the first character of the month is a digit, parse it as an integer
					monthValue = Integer.parseInt(month);
				} else {
					  // Map month names to their corresponding numeric values
					switch (month) {
						case "january":
							monthValue = 1;
							break;
						case "february":
							monthValue = 2;
							break;
						case "march":
							monthValue = 3;
							break;
						case "april":
							monthValue = 4;
							break;
						case "may":
							monthValue = 5;
							break;
						case "june":
							monthValue = 6;
							break;
						case "july":
							monthValue = 7;
							break;
						case "august":
							monthValue = 8;
							break;
						case "september":
							monthValue = 9;
							break;
						case "october":
							monthValue = 10;
							break;
						case "november":
							monthValue = 11;
							break;
						case "december":
							monthValue = 12;
							break;
						default:
							monthValue = 0; // Set a default value or handle invalid inputs
							break;
					}
				}
				
				outkey.set(parts[3]+","+monthValue);
				// Calculate total revenue based on the given formula			
				float totalRevenue = Float.parseFloat(parts[11]) * (Integer.parseInt(parts[7]) + Integer.parseInt(parts[8]));
				String revenueString = Float.toString(totalRevenue);
				outvalue.set(revenueString);
				context.write(outkey,outvalue);
		  }			
        }
    }
	
	

    public static class CombinerReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException

        {
			
		List<Float> maxRevenueList = new ArrayList<>();
        
        // Iterate through all the revenue values for the given year and month
			for (Text value : values) {
				float revenue = Float.parseFloat(value.toString()); 
				
				if (maxRevenueList.isEmpty() || revenue > maxRevenueList.get(0)) {
					maxRevenueList.clear();
					maxRevenueList.add(revenue);
				} else if (revenue == maxRevenueList.get(0)) {
					// If the revenue is equal to the current maximum, add it to the list
					maxRevenueList.add(revenue);
				}
			}
		
		// Sort the maxRevenueList in descending order
        Collections.sort(maxRevenueList, Collections.reverseOrder());
        
         // Emit the year and month as the key and the maxRevenue as the value
        String output = maxRevenueList.isEmpty() ? "" : maxRevenueList.get(0).toString();
        context.write(key, new Text(output));
            
        }
    }

    public static void main(String[] args) throws Exception
    {
        Configuration conf = new Configuration();
        @SuppressWarnings("deprecation")
        Job job = new Job(conf, "Combiner_dataset");
        job.setJarByClass(Combiner.class);
  
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
         
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
  
        job.setMapperClass(CombinerMapper.class);
        job.setReducerClass(CombinerReducer.class);
  
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
  
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        Path out = new Path(args[1]);
        out.getFileSystem(conf).delete(out);
        job.waitForCompletion(true);
    }
}