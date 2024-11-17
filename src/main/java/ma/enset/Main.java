package ma.enset;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class Main {
    public static void main(String[] args) {
        //Master: l execution de l app sera en mode local , * : utiliser thread selon ressource de l app
        SparkConf conf = new SparkConf().setAppName("vente rdd").setMaster("local[*]");

        JavaSparkContext context=new JavaSparkContext(conf);

        //Creer un RDD apartir d un file : total des ventes par ville
        JavaRDD<String> lines = context.textFile("ventes.txt");

        //diviser les lignes sous forme des mots , par map , creer un rdd des villes
        //dans chaque ligne extraire le 1er element de la ligne
        JavaRDD<String> villes = lines.map(line -> line.split(" ")[1]);

        //chaque élément du RDD villes en une paire clé-valeur (ou tuple) (ville,1)
        JavaPairRDD<String, Integer> rddPair=villes.mapToPair(ville->new Tuple2<>(ville,1));
        JavaPairRDD<String,Integer> rddVilleCount= rddPair.reduceByKey((a,b)->a+b);
        rddVilleCount.foreach(elem-> System.out.println(elem._1()+ " "+elem._2()));


    }
}