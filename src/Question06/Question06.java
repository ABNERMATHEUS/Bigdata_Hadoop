package Question06;

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

public class Question06 {

    public static void main(String[] args) throws Exception {

        BasicConfigurator.configure();

        Configuration c = new Configuration();

        Path input = new Path("in/transactions.csv");

        Path output = new Path("output/Question06.txt");

        Job j = new Job(c, "Question06");

        j.setJarByClass(Question06.class);
        j.setMapperClass(MapQuestion06.class);
        j.setReducerClass(ReduceQuestion06.class);
        j.setCombinerClass(CombinerQuestion06.class);


        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(IntWritable.class);


        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        j.waitForCompletion(true);
    }

    public static class MapQuestion06 extends Mapper<LongWritable, Text, Text, ExerciciosTmpWritable> {
        public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {

            String line = value.toString();

            if(line.startsWith("country_or_area")) return;

            String[] column = line.split(";");

            int priceCommodity = Integer.parseInt(column[5]);

            // EMITIR ( "MERCADORIA", MERCADORIA / PREÇO)
            con.write(new Text(informacao[7]), new ExerciciosTmpWritable(informacao[3],Integer.toString(preçoMercadoria)));
        }
    }

    public static class CombinerQuestion06 extends Reducer<Text, ExerciciosTmpWritable, Text, ExerciciosTmpWritable> {
        public void reduce(Text word, Iterable<ExerciciosTmpWritable> values, Context con) throws IOException, InterruptedException {
            // DECLARANDO AS VARIÁVEIS
            int maiorQtde = 0;
            String mercadoria = "";

            // FAZ A BUSCA PELA CATEGORIA COM MAIOR PREÇO
            for(ExerciciosTmpWritable w : values){
                if(Integer.parseInt(w.n) > maiorQtde){
                    maiorQtde = Integer.parseInt(w.n);
                    mercadoria = w.mercadoria;
                }
            }
            // EMITINDO RESULTADOS ("CATEGORIA", NOME DA MERCADORIA, QUANTIDADE DESTA MERCADORIA)
            con.write(word, new ExerciciosTmpWritable(mercadoria,Integer.toString(maiorQtde)));
        }

    }

    public static class ReduceQuestion06 extends Reducer<Text, ExerciciosTmpWritable, Text, ExerciciosTmpWritable> {
        public void reduce(Text word, Iterable<ExerciciosTmpWritable> values, Context con) throws IOException, InterruptedException {
            // DECLARANDO AS VARIÁVEIS
            int maiorQtde = 0;
            String mercadoria = "";

            // FAZ A BUSCA PELA CATEGORIA COM MAIOR PREÇO
            for(ExerciciosTmpWritable w : values){
                if(Integer.parseInt(w.n) > maiorQtde){
                    maiorQtde = Integer.parseInt(w.n);
                    mercadoria = w.mercadoria;
                }
            }
            // EMITINDO RESULTADOS ("CATEGORIA", NOME DA MERCADORIA, QUANTIDADE DESTA MERCADORIA)
            con.write(word, new ExerciciosTmpWritable(mercadoria,Integer.toString(maiorQtde)));
        }
    }

}
