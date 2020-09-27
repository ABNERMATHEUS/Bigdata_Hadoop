package Question03;

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
import java.util.*;

public class MostCommercialCommodity {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        BasicConfigurator.configure();

        Configuration c = new Configuration();

        Path input = new Path("in/transactions.csv");

        Path intermediate = new Path("output/itermediarioQuestion03.txt");

        Path output = new Path("output/Question03.txt");


        Job j = new Job(c, "Question03");

        j.setJarByClass(MostCommercialCommodity.class);
        j.setMapperClass(MapMostCommercialCommodity.class);
        j.setReducerClass(ReduceMostCommercialCommodity.class);

        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, intermediate);

        j.waitForCompletion(true);



        Job j2 = new Job(c,"Question03-prt2");

        j2.setJarByClass(MostCommercialCommodity.class);
        j2.setMapperClass(MapMostCommercialCommodityEtapaB.class);
        j2.setReducerClass(ReduceMostCommercialCommodityEtapaB.class);

        j2.setMapOutputKeyClass(Text.class);
        j2.setMapOutputValueClass(UtilMostCommercialCommodity.class);

        FileInputFormat.addInputPath(j2, intermediate);
        FileOutputFormat.setOutputPath(j2, output);

        j2.waitForCompletion(true);

    }

    public static class MapMostCommercialCommodity extends Mapper<LongWritable, Text, Text, IntWritable> {
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {

            String linha = value.toString();

            if(linha.startsWith("country_or_area")) return;

            String[] column = linha.split(";");

            String year = column[1];
            String type = column[4];
            String codeCommodity = column[2];

            if(!year.equals("2016")) return;
            if(codeCommodity.equals("TOTAL")) return;

            con.write(new Text(type + " " + codeCommodity), new IntWritable(1));
        }
    }


    public static class ReduceMostCommercialCommodity extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text word, Iterable<IntWritable> values, Context con)
                throws IOException, InterruptedException {

            int cont = 0;

            for(IntWritable obj : values) {
                cont++;
            }

            con.write(word, new IntWritable(cont));

        }
    }




    public static class MapMostCommercialCommodityEtapaB extends Mapper<LongWritable, Text, Text, UtilMostCommercialCommodity> {
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {

            String line = value.toString();

            String[] column = line.split("\t");
            String type = column[0].split(" ")[0];
            String codeCommodity = column[0].split(" ")[1];
            int qtde = Integer.parseInt(column[1]);

            con.write(new Text(type), new UtilMostCommercialCommodity(codeCommodity, qtde));
        }
    }

    public static class ReduceMostCommercialCommodityEtapaB extends Reducer<Text, UtilMostCommercialCommodity, Text, UtilMostCommercialCommodity> {
        public void reduce(Text word, Iterable<UtilMostCommercialCommodity> values, Context con)
                throws IOException, InterruptedException {

            String codeCommodity = "vazio";
            int bigger = 0;

            for(UtilMostCommercialCommodity obj : values) {
                if(obj.getCont() > bigger) {
                    bigger = obj.getCont();
                    codeCommodity = obj.getCodeCommodity();
                    System.out.println("chegou");
                }
            }

            con.write(word, new UtilMostCommercialCommodity(codeCommodity, bigger));

        }
    }


}
