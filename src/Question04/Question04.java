package Question04;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;

public class Question04 {

    public static void main(String[] args) throws Exception {

        BasicConfigurator.configure();

        Configuration c = new Configuration();

        Path input = new Path("in/transactions.csv");

        Path output = new Path("output/Question04.txt");

        Job j = new Job(c, "Question04");

        j.setJarByClass(Question04.class);
        j.setMapperClass(MapAverageCommodityYear.class);
        j.setReducerClass(ReduceAverageCommodityYear.class);


        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(FloatWritable.class);


        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        j.waitForCompletion(true);
    }

    public static class MapAverageCommodityYear extends Mapper<LongWritable, Text, Text, FloatWritable> {
        public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {

            String line = value.toString();

            if(line.startsWith("country_or_area")) return;

            String[] column = line.split(";");

            float price = Float.parseFloat(column[5]);
            String year = column[1];
            String codeCommodity = column[2];

            con.write(new Text(year + "  " + codeCommodity), new FloatWritable(price));
        }
    }


    public static class ReduceAverageCommodityYear extends Reducer<Text, FloatWritable, Text, FloatWritable> {
        public void reduce(Text word, Iterable<FloatWritable> values, Context con) throws IOException, InterruptedException {

            float priceCommodity = 0;
            int qtdeCommodity = 0;

            for(FloatWritable w : values){
                priceCommodity += w.get();
                qtdeCommodity += 1;
            }

            float average = priceCommodity / qtdeCommodity;

            con.write(word, new FloatWritable(average));
        }
    }

}
