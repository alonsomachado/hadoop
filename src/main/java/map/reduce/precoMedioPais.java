package map.reduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;



public class precoMedioPais {

    private static class Pair implements Writable {

        private FloatWritable preco;
        private FloatWritable nArtigos;

        public Pair(FloatWritable preco, FloatWritable nArtigos) {
            this.preco = preco;
            this.nArtigos = nArtigos;
        }

        public Pair() {
            this.preco = new FloatWritable(0);
            this.nArtigos = new FloatWritable(0);
        }
        public void set (Pair p){
            this.preco.set(p.getPreco().get());
            this.nArtigos.set(p.getnArtigos().get());      // se for do tipo Text nao leva o ".get()"
        }

        @Override
        public void readFields(DataInput in) throws IOException{
            preco.readFields(in);
            nArtigos.readFields(in);
        }
        @Override
        public void write(DataOutput out) throws IOException{
            preco.write(out);
            nArtigos.write(out);
        }

        public FloatWritable getPreco() {
            return preco;
        }

        public void setPreco(FloatWritable preco) {
            this.preco = preco;
        }

        public FloatWritable getnArtigos() {
            return nArtigos;
        }

        public void setnArtigos(FloatWritable nArtigos) {
            this.nArtigos = nArtigos;
        }

        @Override
        public String toString(){
            return "{" + preco +"," + nArtigos+ "$}";
        }
    }

    //Retailer country,Order method type,Retailer type,Product line,Product type,Product,Year,Quarter,Revenue,Quantity,Gross margin
    public static class MyMapper extends Mapper<Object, Text, Text, Pair>
    {


        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException
        {
            String parts[]= value.toString().split(",");

            if(parts.length == 11){
                try {
                    context.write(new Text(parts[0]),new Pair(new FloatWritable(Float.parseFloat(parts[8])), new FloatWritable(Float.parseFloat(parts[9]))));
                    // parts[8] - revenue
                    // parts[9] - quantity
                }
                catch (NumberFormatException ex){
                    ex.printStackTrace();
                }
            }
        }
    }

    public static class MyReducer extends Reducer<Text,Pair,Text,FloatWritable>{


        @Override
        public void reduce(Text key, Iterable<Pair> values, Context context) throws IOException, InterruptedException
        {
            Float sum=0f,q=0f;
            //Integer q=0;
            // average = total revenue / total quantity
            for(Pair val: values) {
                sum += val.getPreco().get();
                q += val.getnArtigos().get();
            }
            context.write(key, new FloatWritable(sum/q));

        }
    }


    public static void main(String[] args) throws Exception
    {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "precoMedioPais");
        job.setJarByClass(precoMedioPais.class);
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
        //job.setCombinerClass(TotalVendasAno2.MyReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Pair.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

