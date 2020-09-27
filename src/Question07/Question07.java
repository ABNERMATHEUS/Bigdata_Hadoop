package Question07;

import Question01.Question01;
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
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;

public class Question07 {

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();

        Configuration c = new Configuration();

        Path input = new Path("in/transactions.csv");

        Path output = new Path("output/Question01.txt");

        Job j = new Job(c, "Question07");

        j.setJarByClass(Question07.class);
        j.setMapperClass(MapQuestion07.class);
        j.setReducerClass(ReduceQuestion07.class);

        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        j.waitForCompletion(true);
    }

    public static class MapQuestion07 extends Mapper<LongWritable, Text, Text, IntWritable> {
        public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {

            String line = value.toString();
            String[] column = line.split(";");

            if(line.startsWith("country_or_area")) return;

            String year = column[1];
            String flow = column[4];

            con.write(new Text(year + " " + flow), new IntWritable(1));
        }
    }

    public static class ReduceQuestion07 extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text word, Iterable<IntWritable> values, Context con) throws IOException, InterruptedException {

            int sum = 0;

            for (IntWritable obj : values) {
                sum += obj.get();
            }
            con.write(word, new IntWritable(sum));
        }
    }

}
