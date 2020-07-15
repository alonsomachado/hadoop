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
import java.util.TreeMap;


public class produtoPaisMaisCaro {

    private static class CountryPrice implements Writable {

        private FloatWritable avarage;
        private Text country;

        public CountryPrice(FloatWritable avarage, Text country) {
            this.avarage = avarage;
            this.country = country;
        }

        public CountryPrice() {
            this.avarage = new FloatWritable(0);
            this.country = new Text("");
        }

        public void set (CountryPrice p){
            this.avarage.set(p.getAvarage().get());
            this.country.set(p.getCountry());      // se for do tipo Text nao leva o ".get()"
        }

        @Override
        public void readFields(DataInput in) throws IOException{
            avarage.readFields(in);
            country.readFields(in);
        }
        @Override
        public void write(DataOutput out) throws IOException{
            avarage.write(out);
            country.write(out);
        }

        public FloatWritable getAvarage() {
            return avarage;
        }

        public void setAvarage(FloatWritable avarage) {
            this.avarage = avarage;
        }

        public Text getCountry() {
            return country;
        }

        public void setCountry(Text country) {
            this.country = country;
        }

    }

    public static class MyMapper extends Mapper<Object, Text, Text, CountryPrice>
    {
        // (K,V(Pair)) -> (Product-country, avarage )
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException
        {
            String parts[]= value.toString().split("\t");
            String dados[] = parts[0].split("-");
            // dados[0] Product
            // dados[1] Country (PAIS)
            // parts[1] Avarage (MEDIA)

            if(parts.length == 2){
                try {
                    context.write(new Text(dados[0]), new CountryPrice(new FloatWritable(Float.parseFloat(parts[1])), new Text(dados[1]) ) );
                    // (K,V(Pair)) -> (Product, (avarage,country) )
                }
                catch (NumberFormatException ex){
                    ex.printStackTrace();
                }
            }
        }
    }

    public static class MyReducer extends Reducer<Text, CountryPrice, Text, Text>{

        private TreeMap<Float,Text> temperaturaMapRed = new TreeMap<>();
        private CountryPrice cpmax = new CountryPrice();

        @Override
        public void reduce(Text key, Iterable<CountryPrice> values, Context context) throws IOException, InterruptedException
        {
            Float max = 0f;

            for(CountryPrice cp: values) {
                //String parts[] = cp.toString().split(",");
               if(max < cp.getAvarage().get() ) {
                    max = cp.getAvarage().get();
                    cpmax.set(cp);
               }
            }
            context.write(key, new Text(cpmax.getCountry()+"-"+cpmax.getAvarage() ) );
            // (K,V(Pair)) -> (Product, (avarage,country) )
        }
    }


    public static void main(String[] args) throws Exception
    {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Produto pais mais caro");
        job.setJarByClass(produtoPaisMaisCaro.class);

        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
        //job.setCombinerClass(MyReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(CountryPrice.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(CountryPrice.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
