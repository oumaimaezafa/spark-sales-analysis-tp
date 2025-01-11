package ma.oumaimaezafa.CourExamples;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class App2 {
    public static void main(String[] args) {
        //Exemple de Word Count
        SparkConf conf=new SparkConf().setAppName("TP WORD COUNT").setMaster("local[*]");
        JavaSparkContext sc=new JavaSparkContext(conf);

        //charger un fichier text
        JavaRDD<String> rddtxtLines=sc.textFile("word.txt");
        //transformations
        JavaRDD<String> words=  rddtxtLines.flatMap((line)-> Arrays.asList(line.split(" ")).iterator());
        JavaPairRDD<String,Integer>rddPairWords=words.mapToPair(word-> new Tuple2<>(word,1));
        JavaPairRDD<String,Integer> rddwordCount=rddPairWords.reduceByKey((a,b)-> a+b);

        //Actions
        rddwordCount.foreach(elem-> System.out.println(elem._1()+"  "+ elem._2()));

    }
}
