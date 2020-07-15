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

public class precoMedioPaisERRO {

    public static class MyMapper extends Mapper<Object, Text, Text, FloatWritable>
    {
        //private final static IntWritable one = new IntWritable(1);
        //private Text word = new Text();
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException
        {
            String parts[] = value.toString().split(",");
            String parts1[] = value.toString().split("-");
            String parts2[] = parts1[1].split("\t");
            if(parts.length == 11) {
                try {

                    Float valor = Float.parseFloat(parts[8]);
                    Float qnt = Float.parseFloat(parts[9]);
                    FloatWritable media = new FloatWritable();
                    media.set(valor/qnt);
                    context.write(new Text(parts[0]+" --- "), media);
                    //(Key, value) -- (pais, valor) -- (United States, 999)
                    
                    //Usar a Classe PAIR que o professor passou
                    //(Key, value) --> ( pais, (valor,quantidade) ) --> ( United States, (999, 10) )
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
        Job job = Job.getInstance(conf, "Preco Medio Por Pais");
        job.setJarByClass(precoMedioPaisERRO.class);
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
