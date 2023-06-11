
import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Combined {
    public static class StatesMapper extends Mapper<Object, Text, Text, Text> {
        private Text outkey = new Text();
        private Text outvalue = new Text();

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            // Extract necessary columns from the customer-reservations dataset
            String[] data = value.toString().split(",");
           
            String staysInWeekendNights = data[1];
            String staysInWeekNights = data[2];
            String arrivalYear = data[4];
            String arrivalMonth = data[5];
            String avgPricePerRoom = data[8];
            String bookingStatus = data[7];
            
            if (!bookingStatus.equals("1")) {
                outkey.set(bookingId);
                outvalue.set("c," + staysInWeekendNights + "," + staysInWeekNights + "," + arrivalYear + "," + arrivalMonth + "," + avgPricePerRoom);
                context.write(outkey, outvalue);
            }
        }
    }

    public static class StationMapper extends Mapper<Object, Text, Text, Text> {
        private Text outkey = new Text();
        private Text outvalue = new Text();

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String record = value.toString();
            String[] parts = record.split(",");

			if (parts.length > 11 && parts[4].length() > 0 && parts[3].length() > 0 && parts[7].length() > 0 && parts[8].length() > 0 && parts[11].length() > 0)
			{	
				outkey.set(getNumericMonth(parts[4]));
				float totalRevenue = Float.parseFloat(parts[11]) * (Integer.parseInt(parts[7]) + Integer.parseInt(parts[8]));
				outvalue.set("H " + parts[3]+","+ getNumericMonth(parts[4])+","+ totalRevenue);
				context.write(outkey, outvalue);
			}
        }
    }

    public static String getNumericMonth(String month) {
        switch (month.toLowerCase()) {
            case "january":
                return "1";
            case "february":
                return "2";
            case "march":
                return "3";
            case "april":
                return "4";
            case "may":
                return "5";
            case "june":
                return "6";
            case "july":
                return "7";
            case "august":
                return "8";
            case "september":
                return "9";
            case "october":
                return "10";
            case "november":
                return "11";
            case "december":
                return "12";
            default:
                return "";
        }
    }

    public static class CombinedReducer extends Reducer<Text, Text, Text, Text> {
        private ArrayList<Text> lh = new ArrayList<Text>();
		private ArrayList<Text> lc = new ArrayList<Text>();

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
			
			lh.clear();
			lc.clear();
			for (Text t : values) 
			{
				String tmp = t.toString();
				if(tmp.charAt(0) == 'H')
				{
					lh.add(new Text(tmp.substring(2)));
				} 
				else if(tmp.charAt(0) == 'C')
				{
					lc.add(new Text(tmp.substring(2)));
				}
			}
			
			
		
			/*for (Text A : lh){
					context.write(A, new Text());
				}
			for (Text B : lc) {
						context.write(B, new Text());
				}*/
				
			
			
			if(!lc.isEmpty() && !lc.isEmpty()){
				for (Text A : lh){
					for (Text B : lc) {
						context.write(A, B);
					}
				}
			}
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "Reduce-side join");
        job.setJarByClass(Combined.class);
        job.setReducerClass(CombinedReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, StatesMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, StationMapper.class);
        Path outputPath = new Path(args[2]);

        FileOutputFormat.setOutputPath(job, outputPath);
        outputPath.getFileSystem(conf).delete(outputPath);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}