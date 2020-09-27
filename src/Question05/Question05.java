package Question05;

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

public class Question05 {

    public static void main(String[] args) throws Exception {

        BasicConfigurator.configure();

        Configuration c = new Configuration();

        Path input = new Path("in/transactions.csv");

        Path output = new Path("output/Question05.txt");

        Job j = new Job(c, "exercicio5");

        j.setJarByClass(Question05.class);
        j.setMapperClass(MapAveragePriceOfCommoditiesPerUnitType.class);
        j.setReducerClass(ReduceAveragePriceOfCommoditiesPerUnitType.class);
        j.setCombinerClass(CombinerAveragePriceOfCommoditiesPerUnitType.class);

        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        j.waitForCompletion(true);
    }

    public static class MapAveragePriceOfCommoditiesPerUnitType extends Mapper<LongWritable, Text, Text, IntWritable> {
        public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {

            String line = value.toString();

            if(line.startsWith("country_or_area")) return;

            String[] column = line.split(";");

            String country = column[0];
            if(!country.equals("Brazil")) return;



            int mediaPesoMercadoria = 0;
            mediaPesoMercadoria = Integer.parseInt(informacao[6]);

            // EMITIR (ANO + MERCADORIA, PESO)
            con.write(new Text(informacao[1] + " " + informacao[3]), new IntWritable(mediaPesoMercadoria));

        }
    }

    public static class CombinerAveragePriceOfCommoditiesPerUnitType extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text word, Iterable<IntWritable> values, Context con) throws IOException, InterruptedException {
            int pesoMercadoria = 0;
            int qtdeMercadoriaAno = 0;

            // PEGA O PESO TOTAL DE CADA MERCADORIA E A QUANTIDADE DE X QUE HOUVE TRANSAÇÃO DESSA MERCADORIA NO ANO
            // ENTÃO DIVIDE PESO DA MERCADORIA PELA QTDE DE X QUE APARECEU NO ANO
            for(IntWritable w : values){
                pesoMercadoria += w.get();
                qtdeMercadoriaAno += 1;
            }
            // EMITIR RESULTADO (PESO DE CADA MERCADORIA / QUANTIDADE DE TRANSAÇOES DESSA MERCADORIA POR ANO)
            con.write(word, new IntWritable(pesoMercadoria/qtdeMercadoriaAno));
        }
    }

    public static class ReduceAveragePriceOfCommoditiesPerUnitType extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text word, Iterable<IntWritable> values, Context con) throws IOException, InterruptedException {
            int pesoMercadoria = 0;
            int qtdeMercadoriaAno = 0;

            // PEGA O PESO TOTAL DE CADA MERCADORIA E A QUANTIDADE DE X QUE HOUVE TRANSAÇÃO DESSA MERCADORIA NO ANO
            // ENTÃO DIVIDE PESO DA MERCADORIA PELA QTDE DE X QUE APARECEU NO ANO
            for(IntWritable w : values){
                pesoMercadoria += w.get();
                qtdeMercadoriaAno += 1;
            }
            // EMITIR RESULTADO (PESO DE CADA MERCADORIA / QUANTIDADE DE TRANSAÇOES DESSA MERCADORIA POR ANO)
            con.write(word, new IntWritable(pesoMercadoria/qtdeMercadoriaAno));
        }
    }

}
