package bigdata.hbase.tp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.*;

/**
 * Classe pour traiter les données HBase avec Spark.
 * Lit la table "products" depuis HBase, crée un RDD en mémoire,
 * et compte le nombre d'enregistrements à l'aide d'un job Spark.
 */
public class HbaseSparkProcess {

    /**
     * Méthode principale pour lire la table HBase et compter les éléments avec Spark.
     * Configure HBase, crée un RDD à partir de la table "products",
     * et affiche le nombre d'enregistrements.
     */
    public void processHBaseTable() {
        // Configuration HBase pour se connecter à la base
        Configuration config = HBaseConfiguration.create();

        // Configuration Spark pour l'application
        SparkConf sparkConf = new SparkConf()
                .setAppName("SparkHBaseTest")  // Nom de l'application Spark
                .setMaster("local[4]");        // Mode local avec 4 threads

        // Création du contexte Spark
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        // Spécifier la table HBase à lire
        config.set(TableInputFormat.INPUT_TABLE, "products");

        // Créer un RDD à partir de la table HBase
        // Utilise TableInputFormat pour lire les données HBase
        JavaPairRDD<ImmutableBytesWritable, Result> hBaseRDD = jsc.newAPIHadoopRDD(
                config, TableInputFormat.class, ImmutableBytesWritable.class, Result.class);

        // Compter le nombre d'enregistrements dans le RDD
        System.out.println("Nombre d'enregistrements: " + hBaseRDD.count());

        // Fermer le contexte Spark
        jsc.close();
    }

    /**
     * Méthode principale pour exécuter le traitement.
     */
    public static void main(String[] args) {
        HbaseSparkProcess processor = new HbaseSparkProcess();
        processor.processHBaseTable();
    }
}