package transactions;

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

public class TransactionsBrazil {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        BasicConfigurator.configure();

        Configuration c = new Configuration();

        Path input = new Path("in/transactions.csv");

        Path output = new Path("output/01TransactionsBrazil.txt");

        Job j = new Job(c, "transactions-brazil");

        j.setJarByClass(TransactionsBrazil.class);
        j.setMapperClass(MapTransactionsBrazil.class);
        j.setReducerClass(ReduceTransactionsBrazil.class);

        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        j.waitForCompletion(true);

    }

    public static class MapTransactionsBrazil extends Mapper<LongWritable, Text, Text, IntWritable> {
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {

            String linha = value.toString();

            if(linha.startsWith("country_or_area")) return;

            String[] column = linha.split(";");

            if(column[0].equals("Brazil")) {
                con.write(new Text("Brazil"), new IntWritable(1));
            }
        }
    }

    public static class ReduceTransactionsBrazil extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text word, Iterable<IntWritable> values, Context con)
                throws IOException, InterruptedException {

            int soma = 0;

            for(IntWritable obj: values) {
                soma += obj.get();
            }

            con.write(new Text("Brazil"), new IntWritable(soma));

        }
    }

}
