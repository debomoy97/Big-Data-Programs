package com.hadoop.aggregate;



import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Aggregate {

  public static class TokenizerMapper 
       extends Mapper<Object, Text, Text, Text>{
    
   
    private Text point=new Text();
    private Text place=new Text();
    
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      
      int lati=0,longi=0;
      while (itr.hasMoreTokens()) {
        String temp1=itr.nextToken();
        if(itr.hasMoreTokens())
        {
    	String temp=itr.nextToken();
    	
    	if(temp.matches(".*point"))
    	{	
    		try{
    			lati=(int)(Double.parseDouble(itr.nextToken()));

    			longi=(int)(Double.parseDouble(itr.nextToken()));
    			point.set(Integer.toString(lati)+" "+Integer.toString(longi));
    			place.set(temp1);
    			context.write(point,place);
    		}
    	catch(NumberFormatException e)
    	{
    		continue;
    	}
    	}		
        }
      }
    }
  }
 
  
  public static class IntSumReducer 
       extends Reducer<Text,Text,Text,Text> {
    private Text result = new Text();

    public void reduce(Text key, Iterable<Text> values, 
                       Context context
                       ) throws IOException, InterruptedException {
      
      String sum="";
      for (Text val : values) {
        
        sum=sum+val.toString()+"	";
      }
      result.set(sum);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length < 2) {
      System.err.println("Usage: aggregate <in> [<in>...] <out>");
      System.exit(2);
    }
    Job job = Job.getInstance(conf, "aggregate");
    job.setJarByClass(Aggregate.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    for (int i = 0; i < otherArgs.length - 1; ++i) {
      FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
    }
    FileOutputFormat.setOutputPath(job,
      new Path(otherArgs[otherArgs.length - 1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
