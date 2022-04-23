package spark.java;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;
import scala.Tuple2;
import spark.java.model.Employee;
import spark.java.model.Gender;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public interface IHelper {
    Path pathData = Path.of("data");

    String employeeJsonFile = "employee.json";

    // When spark.master is not set, we can run the application locally
    SparkConf conf = new SparkConf().setIfMissing("spark.master", "local[*]");

    default SparkSession getOrCreateSpark() {
        // Create a spark session, entry point to all of Spark's functionality
        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

        // Change the log level to hide INFO messages from Spark
        spark.sparkContext().setLogLevel("ERROR");

        return spark;
    }

    default String createPath(String fileName) {
        return pathData.resolve(fileName).toString();
    }

    default void printDataset(Dataset<?> dataset) {
        // Print dataset schema
        dataset.printSchema();

        // Show dataset, up to 20 rows.
        dataset.show();
    }

    default Dataset<Row> createEmployeeDataset(SparkSession spark) {
        return spark.read().option("multiline", true).json(createPath(employeeJsonFile));
    }

    // Create department Dataset from a java List
    default Dataset<Row> createDepartmentDataset(SparkSession spark) {
        List<Tuple2<String, String>> list = Arrays.asList(
                new Tuple2<>("Operations", "Tom"),
                new Tuple2<>("Administration", "John"),
                new Tuple2<>("IT", "Susan"),
                new Tuple2<>("Operations", "Robert"),
                new Tuple2<>("Administration", "Ella"),
                new Tuple2<>("IT", "Chris"),
                new Tuple2<>("Operations", "Thomas"),
                new Tuple2<>("Administration", "Frank"),
                new Tuple2<>("IT", "Mia"),
                new Tuple2<>("IT", "Lucy"),
                new Tuple2<>("IT", "SuperMan")
        );

        return spark.createDataset(list, Encoders.tuple(Encoders.STRING(), Encoders.STRING()))
                .toDF("name", "employee_name");
    }

    default void createAndWriteEmployeeDataset(SparkSession spark) {
        List<Employee> employees = new ArrayList<>();

        employees.add(new Employee("Tom", 20, Gender.male));
        employees.add(new Employee("John", 25, Gender.male));
        employees.add(new Employee("Susan", 30, Gender.female));
        employees.add(new Employee("Robert", 35, Gender.male));
        employees.add(new Employee("Ella", 40, Gender.female));
        employees.add(new Employee("Chris", 45, Gender.male));
        employees.add(new Employee("Thomas", 50, Gender.male));
        employees.add(new Employee("Frank", 55, Gender.male));
        employees.add(new Employee("Mia", 60, Gender.female));
        employees.add(new Employee("Lucy", 65, Gender.female));

        Dataset<Employee> dataset = spark.createDataset(employees, Encoders.bean(Employee.class));

        printDataset(dataset);

        // Create a local output path for test purpose.
        // In production environment, a cloud storage is probably the best choice
        String jsonOutputPath = IHelper.pathData.resolve("employee").toString();

        // Combine the result into one json file, allow overwriting the existing data
        dataset.coalesce(1).write().mode(SaveMode.Overwrite).json(jsonOutputPath);
    }
}
