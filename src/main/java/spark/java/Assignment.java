package spark.java;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import spark.java.model.Employee;

public class Assignment implements IHelper, Runnable {
    public static void main(String[] args) {
        new Assignment().run();
    }

    @Override
    public void run() {
        SparkSession spark = getOrCreateSpark();

        Dataset<Row> department = assignment1(createPath("department.csv"), spark);

        // Print the schema and show department data table, up to 20 rows.
        printDataset(department);

        assignment2(department, spark);

        assignment3(department, createEmployeeDataset(spark).as(Encoders.bean(Employee.class)), spark);
    }

    /**
     * Create a department Dataset from given spark and pathDepartmentCsvFile
     *
     * @param pathDepartmentCsvFile The path of the department csf file.
     * @param spark                 The spark session.
     * @return The department Dataset read from the given path.
     */
    public Dataset<Row> assignment1(String pathDepartmentCsvFile, SparkSession spark) {
        // TODO

        return null;
    }

    /**
     * Show number of employees in every department, order by number of employees in descending order
     *
     * @param department The department Dataset.
     */
    public void assignment2(Dataset<Row> department, SparkSession spark) {
        // TODO, do it in Spark SQL way

        // TODO, do it in Spark API way
    }

    /**
     * Show department name with the youngest employee
     *
     * @param department The department Dataset.
     * @param employee   The employee Dataset.
     */
    public void assignment3(Dataset<Row> department, Dataset<Employee> employee, SparkSession spark) {
        // TODO, do it in Spark SQL way

        // TODO, do it in Spark API way
    }
}
