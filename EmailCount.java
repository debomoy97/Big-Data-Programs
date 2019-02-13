/*--------------------------------------------------------------------------------------------------------------------------
MAPREDUCE PROBLEM STATEMENT 1
--------------------------------------------------------------------------------------------------------------------------

#1. A list of multiple email addresses is provided in this folder.
#2. The format of the data is like this:
					
user1@domain1.com+"\n"+user2@domain2.com+"\n"+user3@domain3.com+"\n"+........................

#3. You have to count the number of hotmail, yahoo and gmail users from this data. */


/*Solution program by Ankur Karmakar*/
package com.hadoop.mapred;



import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class EmailCount {

  public static class TokenizerMapper 
       extends Mapper<Object, Text, Text, IntWritable>{
    
    private final static IntWritable one = new IntWritable(1);
    
    private Text hotmail=new Text();
    private Text yahoo=new Text();
    private Text gmail=new Text();
    
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
       
    	String temp=itr.nextToken();
    	if(temp.matches(".*@hotmail.com"))
    	{
    		hotmail.set("hotmail");//setting key as "hotmail"
    		context.write(hotmail, one);//writing key-value pairs
    	}
    	else if(temp.matches(".*@yahoo.com"))
    	{
    		yahoo.set("yahoo");//setting key as "yahoo"
    		context.write(yahoo, one);//writing key-value pairs
    	}
    	else if(temp.matches(".*@gmail.com"))
    	{
    		gmail.set("gmail");//setting key as "gmail"
    		context.write(gmail, one);//writing key-value pairs
    	}
      }
    }
  }
  /*After mapping the data is mapped into key-value pairs like these=
    <gmail,1>
    <hotmail,1>
    <yahoo,1>
    <gmail,1>
    <hotmail,1>
    <yahoo,1>
    */
  
  public static class IntSumReducer 
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values, 
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();//getting the sum of values of same key
      }
      result.set(sum);
      context.write(key, result);//writing the sum of values of same keys
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length < 2) {
      System.err.println("Usage: wordcount <in> [<in>...] <out>");
      System.exit(2);
    }
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(EmailCount.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    for (int i = 0; i < otherArgs.length - 1; ++i) {
      FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
    }
    FileOutputFormat.setOutputPath(job,
      new Path(otherArgs[otherArgs.length - 1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
