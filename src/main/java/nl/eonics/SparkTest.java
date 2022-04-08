package nl.eonics;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkTest {
    public static void main(String[] args) throws InterruptedException {
        SparkSession spark = SparkSession.builder().master("local[*]").getOrCreate();

        Dataset<Row> data = spark.read().option("header", true)
                .csv("src/main/resources/city.csv");
        data.printSchema();
        data.show();

        data = data.select("city").where("country = 'Netherlands'");
        data.printSchema();
        data.show();

        Thread.sleep(60000);
    }
}
