package ma.enset;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

public class App2 {
    public static void main(String[] args) {
        // Master: l'exécution de l'application sera en mode local, * : utiliser des threads selon les ressources de l'application
        SparkConf conf = new SparkConf().setAppName("vente rdd").setMaster("local[*]");
        JavaSparkContext context = new JavaSparkContext(conf);

        // Année à filtrer (argument passé en ligne de commande)
        if (args.length < 1) {
            System.out.println("Veuillez spécifier une année !");
            System.exit(1);
        }
        String annee = args[0];

        // Créer un RDD à partir d'un fichier : ventes.txt
        JavaRDD<String> lines = context.textFile("ventes.txt");

        // Filtrer les lignes correspondant à l'année donnée
        JavaRDD<String> filteredLines = lines.filter(line -> line.startsWith(annee));

        // Mapper les lignes pour obtenir les tuples (ville, prix)
        JavaRDD<Tuple2<String, Double>> villePrixRDD = filteredLines.map(line -> {
            String[] parts = line.split(" ");
            String ville = parts[1]; // Ville
            double prix = Double.parseDouble(parts[3]); // Prix
            return new Tuple2<>(ville, prix);
        });

        // Calculer le total des ventes par ville
        JavaRDD<Tuple2<String, Double>> totalParVilleRDD = villePrixRDD
                .mapToPair((PairFunction<Tuple2<String, Double>, String, Double>) tuple -> new Tuple2<>(tuple._1(), tuple._2()))
                .reduceByKey((x, y) -> x + y)
                .map(tuple -> new Tuple2<>(tuple._1, tuple._2));

        // Afficher les résultats
        totalParVilleRDD.collect().forEach(result -> {
            System.out.println("Ville: " + result._1 + ", Total des ventes: " + result._2);
        });

        // Fermer le contexte
        context.close();
    }
}
