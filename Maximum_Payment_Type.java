package sales;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Maximum_Payment_Type {
	
	//Mapper Class
	public static class MapForMaxPaymentType extends Mapper<LongWritable, Text, Text, IntWritable>{
		
		public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException{
			
			if (key.get() == 0){
				return;
			} else{
				String line = value.toString();
				String[] words = line.split(",");	
				Text outputkey = new Text(words[3]);
				IntWritable outputvalue = new IntWritable(1);
				con.write(outputkey, outputvalue);
			}
		}
	}
	
	
	//Reducer Class
	public static class ReduceForMaxPaymentType extends Reducer<Text, IntWritable, Text, IntWritable>{
		//Creating instance of the hashmap
		Map<Text, IntWritable> map = new HashMap<>();
		
		public void reduce(Text word, Iterable<IntWritable> values, Context con) throws IOException, InterruptedException{
			
			int sum = 0;
			for(IntWritable value: values){
				sum = sum + value.get();
			}
			
			//adding all the values to the map
			map.put(word, new IntWritable(sum));
		}
		
		public void cleanup(Context con) throws IOException, InterruptedException{
			Map.Entry<Text, IntWritable> paymentWithMaxValue = null; 
			  
	        // Iterate in the map to find the required entry 
	        for (Map.Entry<Text, IntWritable> currentEntry : map.entrySet()) { 
	  
	            if ( 
	                // If this is the first entry, set result as this 
	            		paymentWithMaxValue == null
	  
	                // If this entry's value is more than the max value 
	                // Set this entry as the max 
	                || currentEntry.getValue() 
	                           .compareTo(paymentWithMaxValue.getValue()) 
	                       > 0) { 
	  
	            	paymentWithMaxValue = currentEntry; 
	            } 
	        } 
	        //Writing maxvalue to the file
	        con.write(paymentWithMaxValue.getKey(), paymentWithMaxValue.getValue());
		}
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
		
		Configuration c= new Configuration();
		Job j = Job.getInstance(c, "maxpaymenttype:");
		j.setJarByClass(Maximum_Payment_Type.class);
		j.setMapperClass(MapForMaxPaymentType.class);
		j.setReducerClass(ReduceForMaxPaymentType.class);
		j.setOutputKeyClass(Text.class);
		j.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(j, new Path(args[0]));
		FileOutputFormat.setOutputPath(j, new Path(args[1]));
		System.exit(j.waitForCompletion(true)?0:1);
	}

}
