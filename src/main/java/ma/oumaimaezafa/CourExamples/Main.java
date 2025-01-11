package ma.oumaimaezafa.CourExamples;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

public class Main {
    public static void main(String[] args) {

        SparkConf conf=new SparkConf().setAppName("TP1 RDD").setMaster("local[*]");
        JavaSparkContext sc=new JavaSparkContext(conf);

        List<Integer> values= Arrays.asList(12,10,18,19,1,3,5,90,9);
        JavaRDD<Integer> rdd1= sc.parallelize(values);
        //Appliquer des transformations
        JavaRDD<Integer> rdd2 =rdd1.map(elem-> elem+1);
        JavaRDD<Integer> rdd3=rdd2.filter(elem-> elem>10);

        //Actions
        List<Integer>result=rdd3.collect();
        result.forEach(System.out::println);


    }




}
