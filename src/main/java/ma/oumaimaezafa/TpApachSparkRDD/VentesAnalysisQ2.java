package ma.oumaimaezafa.TpApachSparkRDD;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class VentesAnalysisQ2 {
    public static void main(String[] args) {
        SparkConf conf =new SparkConf().setAppName("Ventes Analysis q2").setMaster("local[*]");

        JavaSparkContext sc=new JavaSparkContext(conf);

        JavaRDD<String> ventesFile=sc.textFile("ventes.txt");

        /*
        * calculer le prix total des ventes des produits par ville pour une année donnée.
        * */
        JavaPairRDD<String, Double> villePrixRdd= ventesFile.mapToPair((line)->{

            String []parts= line.split(" ");
            String ville=parts[1];
            Double price=Double.parseDouble(parts[3]);
            String date= parts[0];

            String annee=date.substring(0,4);

            return new Tuple2<>(ville+"---"+annee,price);
        });

        JavaPairRDD<String,Double> totalVentesVilleAnnee=villePrixRdd.reduceByKey((a,b)->a+b);

        //Affichage de results : Actions
        System.out.println("Total des ventes par ville et par année :");
        totalVentesVilleAnnee.foreach(elem-> System.out.println(elem._1()+" "+elem._2()));




    }
}
