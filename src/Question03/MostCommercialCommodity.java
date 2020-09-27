package Question03;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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

        Path output = new Path("output/Question03.txt");


        Job j = new Job(c, "Question03");

        j.setJarByClass(MostCommercialCommodity.class);
        j.setMapperClass(MapMostCommercialCommodity.class);
        j.setReducerClass(ReduceMostCommercialCommodity.class);
        j.setCombinerClass(CombinerMostCommercialCommodity.class);

        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(UtilMostCommercialCommodity.class);

        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        j.waitForCompletion(true);

    }

    public static class MapMostCommercialCommodity extends Mapper<LongWritable, Text, Text, UtilMostCommercialCommodity> {
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

            con.write(new Text(type), new UtilMostCommercialCommodity(codeCommodity, 1));
        }
    }

    public static class CombinerMostCommercialCommodity extends Reducer<Text, UtilMostCommercialCommodity, Text, UtilMostCommercialCommodity> {
        public void reduce(Text word, Iterable<UtilMostCommercialCommodity> values, Context con)
                throws IOException, InterruptedException {

            List<String> codes = new ArrayList<>();
            String codeAnterior = "";

            for(UtilMostCommercialCommodity obj : values){
                if(!codeAnterior.equals(obj.getCodeCommodity()))
                    codes.add(obj.getCodeCommodity());

                codeAnterior = obj.getCodeCommodity();
            }

            Collections.sort(codes);

            int cont = 0;
            for(String c : codes) {
                cont = 0;
                for(UtilMostCommercialCommodity obj : values) {
                    if(obj.getCodeCommodity().equals(c)) {
                        cont++;
                    }
                }
                con.write(word, new UtilMostCommercialCommodity(c, cont));
            }

        }

    }


    public static class ReduceMostCommercialCommodity extends Reducer<Text, UtilMostCommercialCommodity, Text, UtilMostCommercialCommodity> {
        public void reduce(Text word, Iterable<UtilMostCommercialCommodity> values, Context con)
                throws IOException, InterruptedException {

            int maior = 0;
            UtilMostCommercialCommodity maiorCommodity = new UtilMostCommercialCommodity();

            for(UtilMostCommercialCommodity obj : values) {
                if(obj.getCont() > maior) {
                    maiorCommodity = obj;
                    maior = obj.getCont();
                }
            }

            con.write(word, maiorCommodity);

        }
    }


}
