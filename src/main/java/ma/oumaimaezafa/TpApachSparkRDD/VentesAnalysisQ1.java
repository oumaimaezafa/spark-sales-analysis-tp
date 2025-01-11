package ma.oumaimaezafa.TpApachSparkRDD;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class VentesAnalysisQ1 {
    public static void main(String[] args) {

        SparkConf conf=new SparkConf().setAppName("Ventes Analysis").setMaster("local[*]");

        JavaSparkContext sc= new JavaSparkContext(conf);

         /*
          1-le total des ventes par ville
          charger le fichier texte :
          **/
        JavaRDD<String> readFile=sc.textFile("ventes.txt");

        JavaPairRDD<String,Double> villePriceRDD=readFile.mapToPair((line)->{

            String [] parts= line.split(" ");
            String ville=parts[1];
            Double price=Double.parseDouble(parts[3]);
            return new Tuple2<>(ville,price);

        });
        JavaPairRDD<String,Double> totaleVentesParVille=villePriceRDD.reduceByKey((a,b)->a+b);

        System.out.println("Total des ventes par ville");
        totaleVentesParVille.foreach((elem)-> System.out.println(elem._1()+" : "+elem._2()));


    }
}
