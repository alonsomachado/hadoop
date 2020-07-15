package map.reduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.TreeMap;

public class top10TemperaturasRegistadas {

    public static class MyMapper extends Mapper<Object, Text, NullWritable, Text>
    {
        private TreeMap<Float,Text> temperaturaMap = new TreeMap<>();
        /*private String pais;
        //Args[2] vai ser o pais
        @Override
        protected void setup(Mapper<Object, Text, NullWritable, FloatWritable>.Context context) throws IOException, InterruptedException
        {
            super.setup(context);
            pais = context.getConfiguration().get("pais");
        }*/
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException
        {
            String parts[] = value.toString().split(",");
            //String data[] = parts[0].split("-");
            if(parts.length == 7 && parts[1].length() >= 1) {
                try {
                    //Adiciona na Lista a temperatura e a linha nova
                    //(Key, value) -- (temperatura, Linha completa)
                    temperaturaMap.put(Float.parseFloat(parts[1]),new Text(value));
                    if(temperaturaMap.size() > 10)
                        //Remove da Lista a (Key, value) de menor temperatura a primeira do TreeMap
                        temperaturaMap.remove(temperaturaMap.firstKey());

                }catch (NumberFormatException ex){
                    ex.printStackTrace();
                }
            }
        }
        @Override
        protected void cleanup(Mapper<Object, Text, NullWritable, Text>.Context context) throws IOException, InterruptedException
        {
            super.cleanup(context);
            for(Text t : temperaturaMap.values()){
                context.write(NullWritable.get(),t);
            }
        }
    }

    public static class myReducer extends Reducer<NullWritable,Text,NullWritable,Text>
    {
        private TreeMap<Float,Text> temperaturaMapRed = new TreeMap<>();
        //private IntWritable result = new IntWritable();
        @Override
        public void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException
        {
            for(Text t : values) {
                String parts[] = t.toString().split(",");
                if(parts.length == 7 && parts[1].length() >= 1) {
                    try {
                        //Adiciona na Lista a temperatura e a linha nova
                        //(Key, value) -- (temperatura, Linha completa)
                        temperaturaMapRed.put(Float.parseFloat(parts[1]),new Text(t));
                        if(temperaturaMapRed.size() > 10)
                            //Remove da Lista a (Key, value) de menor temperatura a primeira do TreeMap
                            temperaturaMapRed.remove(temperaturaMapRed.firstKey());

                    }catch (NumberFormatException ex){
                        ex.printStackTrace();
                    }

                }
            }
            for(Text t : temperaturaMapRed.descendingMap().values()) {
                context.write(NullWritable.get(),t);
            }
        }
    }

    public static void main(String[] args) throws Exception
    {
        Configuration conf = new Configuration();
        //conf.set("pais",args[2]);
        Job job = Job.getInstance(conf, "Top N - TOP10 Temperaturas ja Registradas ");
        job.setJarByClass(top10TemperaturasRegistadas.class);
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(myReducer.class);
        //job.setCombinerClass(myReducer.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}