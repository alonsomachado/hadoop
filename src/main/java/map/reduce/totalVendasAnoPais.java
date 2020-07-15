package map.reduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class totalVendasAnoPais {
    public static class MyMapper extends Mapper<Object, Text, Text, FloatWritable>
    {
        //private final static IntWritable one = new IntWritable(1);
        //private Text word = new Text();
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException
        {
            String parts[] = value.toString().split(",");
            if(parts.length == 11) {
                try {
                    context.write(new Text(parts[0]+"-"+parts[6]), new FloatWritable(Float.parseFloat(parts[8])));
                    //(Key, value) -- (pais - ano, valor)
                }catch (NumberFormatException ex){
                    ex.printStackTrace();
                }
            }
        }
    }

    public static class myReducer extends Reducer<Text,FloatWritable,Text,FloatWritable>
    {
        //private IntWritable result = new IntWritable();
        @Override
        public void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException
        {
            float sum = 0f;
            for (FloatWritable val : values)
            {
                sum += val.get();
            }
            //result.set(sum);
            context.write(key, new FloatWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception
    {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Total Vendas Ano por Pais por Ano");
        job.setJarByClass(totalVendasAnoPais.class);
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(myReducer.class);
        job.setCombinerClass(myReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
