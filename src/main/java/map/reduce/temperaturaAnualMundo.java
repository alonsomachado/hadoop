package map.reduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class temperaturaAnualMundo
{

  public static class MyMapper extends Mapper<Object, Text, IntWritable, FloatWritable>
  {
    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
    {
      String parts[] = value.toString().split(",");
      
      if (parts.length == 7 && parts[1].length() > 0)
      {
    	  //ano, temperatura
    	  try{
    		  context.write(new IntWritable(Integer.parseInt(parts[0].split("-")[0])), new FloatWritable(Float.parseFloat(parts[1])));  
    	  }
    	  catch(NumberFormatException ex){}
    	  
      }
    }
  }

  public static class MyReducer extends Reducer<IntWritable,FloatWritable,IntWritable,FloatWritable> 
  {
    @Override
    public void reduce(IntWritable key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException 
    {
    	int cont = 0;
		Float sum = 0f;
		
		for (FloatWritable val : values)
		{
			sum += val.get();
			cont++;
		}
      
		context.write(key, new FloatWritable(sum / cont));
    }
  }
  

  public static void main(String[] args) throws Exception 
  {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "precoMedioPais");
    job.setJarByClass(temperaturaAnualMundo.class);
    job.setMapperClass(MyMapper.class);
    //job.setCombinerClass(MyReducer.class);
    job.setReducerClass(MyReducer.class);
    
    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(FloatWritable.class);
    
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(FloatWritable.class);
    
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}

