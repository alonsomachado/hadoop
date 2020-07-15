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

public class temperaturaAnualMundoMeu {

    public static class MyMapper extends Mapper<Object, Text, Text, FloatWritable>
    {
        //private final static IntWritable one = new IntWritable(1);
        //private Text word = new Text();
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException
        {
            String parts[] = value.toString().split(",");
            String data[] = parts[0].split("-");
            if(parts.length == 7 && parts[1].length() >= 1) {
                try {
                    context.write(new Text(data[0]), new FloatWritable(Float.parseFloat(parts[1])));
                    //(Key, value) -- (ano, valor)
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
            int qnt = 0;
            for (FloatWritable val : values)
            {
                sum += val.get();
                qnt = qnt+1;
            }
            //result.set(sum);
            context.write(key, new FloatWritable(sum/qnt));
        }
    }

    public static void main(String[] args) throws Exception
    {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Temperatua Anual no Mundo Todo");
        job.setJarByClass(temperaturaAnualMundoMeu.class);
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(myReducer.class);
        //job.setCombinerClass(myReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}


