package Question02;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;

public class Question02 {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        BasicConfigurator.configure();

        Configuration c = new Configuration();

        Path input = new Path("in/transactions.csv");

        Path output = new Path("output/Question02.txt");

        Job j = new Job(c, "Question02");

        j.setJarByClass(Question02.class);
        j.setMapperClass(MapNumberPerYear.class);
        j.setReducerClass(ReduceNumberPerYear.class);

        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        j.waitForCompletion(true);

    }

    public static class MapNumberPerYear extends Mapper<LongWritable, Text, Text, IntWritable> {
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {

            String line = value.toString();

            if(line.startsWith("country_or_area")) return;

            String[] column = line.split(";");
            String country = column[1];

            con.write(new Text(country), new IntWritable(1));
        }
    }

    public static class ReduceNumberPerYear extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text word, Iterable<IntWritable> values, Context con)
                throws IOException, InterruptedException {

            int sum = 0;

            for(IntWritable obj: values) {
                sum += obj.get();
            }

            con.write(word, new IntWritable(sum));

        }
    }

}
