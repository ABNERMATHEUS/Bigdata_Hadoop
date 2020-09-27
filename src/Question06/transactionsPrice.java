package Question06;

import Question05.transactionsOverall;
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

public class transactionsPrice {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        BasicConfigurator.configure();

        Configuration c = new Configuration();

        Path input = new Path("in/transactions.csv");

        Path output = new Path("output/06TransactionPrecoMaior.txt");

        Job j = new Job(c, "transactions-brazil6");

        j.setJarByClass(transactionsOverall.class); //CLASSE PRINCIPAL
        j.setMapperClass(MapTransactionsOverall.class);//REGISTRAR DA CLASSE MAP
        j.setReducerClass(ReduceTransactionsOverall.class); //REGISTRO DA CLASSE REDUCE

        j.setMapOutputKeyClass(Text.class);//SAIDA CHAVE MAP
        j.setMapOutputValueClass(Auxiliar.class);// SAIDA VALOR MAP

        j.setCombinerClass(Combiner.class);// Combiner

        j.setOutputKeyClass(Text.class); // SAIDA REDUCE CHAVE
        j.setOutputValueClass(FloatWritable.class); // SAIDA REDUCE VALOR

        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        j.waitForCompletion(true);


    }

    public static class MapTransactionsOverall extends Mapper<LongWritable,Text, Text, Auxiliar>{

        public void map(LongWritable key,Text value, Context con ) throws IOException, InterruptedException {

            String linha =  value.toString();

            if(linha.startsWith("country_or_area"))return;

            String[] colunas = linha.split(";");

            Integer ano = Integer.parseInt(colunas[1]);//pegando o ano
            String tipo = colunas[2];// pegando o tipo
            Float preco = Float.parseFloat(colunas[5]);//preco


            if(colunas[0].equals("Brazil")){
                con.write(new Text("Tipo="+tipo+" Ano= "+ano+" precoMaior= "),new Auxiliar(1,preco));
            }

        }

    }

    public static class ReduceTransactionsOverall extends Reducer<Text, Auxiliar,Text, FloatWritable>{
        public void reduce(Text key, Iterable<Auxiliar> values, Context con) throws IOException, InterruptedException {
            double precoMaior=0;

            for(Auxiliar obj: values){
                if(precoMaior<obj.getPreco()){
                    precoMaior= obj.getPreco();
                }
            }



            con.write(key,new FloatWritable((float) precoMaior));

        }
    }

    public static class Combiner extends Reducer<Text, Auxiliar,Text, Auxiliar>{
        public void reduce(Text word, Iterable<Auxiliar> values, Context con) throws IOException, InterruptedException {

            double precoMaior=0;

            int totalLinhas= 0;
            for (Auxiliar obj: values){
                if(precoMaior<obj.getPreco()){
                    precoMaior= obj.getPreco();
                }
                totalLinhas++;
            }

            con.write(word,new Auxiliar(totalLinhas, (float) precoMaior));

        }
    }



}
