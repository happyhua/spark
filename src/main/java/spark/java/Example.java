package spark.java;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import spark.java.model.Employee;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;

public class Example implements IHelper, Runnable {
    public static void main(String[] args) {
        new Example().run();
    }

    @Override
    public void run() {
        SparkSession spark = getOrCreateSpark();

        // Create employee Dataset from a json file
        Dataset<Employee> employee = createEmployeeDataset(spark).as(Encoders.bean(Employee.class));

        // Print the schema and show employee data table, up to 20 rows.
        printDataset(employee);

        filter(employee, spark);

        aggregate(employee, spark);

        // Create an embedded department Dataset
        Dataset<Row> department = createDepartmentDataset(spark);
        printDataset(department);

        join(employee, department, spark);

        // Create an embedded employee Dataset and write it to a storage
        createAndWriteEmployeeDataset(spark);
    }

    public void filter(Dataset<Employee> employee, SparkSession spark) {
        // Filter employee Dataset with a condition on age column, in Spark SQL way
        // You need to create a temp view to use the Dataset in SQL!
        employee.createOrReplaceTempView("employee");
        System.out.println("select * from employee where age <= 50, in Spark SQL way.");
        spark.sql("select * from employee where age <= 50").show();

        // Filter age column again, but in Spark API way
        System.out.println("select * from employee where age <= 50, in Spark API way.");
        employee.filter(col("age").leq(lit(50))).show();
    }

    public void aggregate(Dataset<Employee> employee, SparkSession spark) {
        // Group employee Dataset by gender column and count the employee size of each gender, in Spark SQL way
        System.out.println("select gender, count(*) from employee group by gender, in Spark SQL way");
        spark.sql("select gender, count(*) from employee group by gender").show();

        // Group gender again, but in Spark API way
        System.out.println("select gender, count(*) from employee group by gender, in Spark API way");
        employee.groupBy("gender").count().show();
    }

    public void join(Dataset<Employee> employee, Dataset<Row> department, SparkSession spark) {
        // Do an inner join between department Dataset and employee Dataset, based on employee_name, in Spark SQL way
        // You need to create a temp view to use the Dataset in SQL!
        department.createOrReplaceTempView("department");
        printDataset(spark.sql("select * from department join employee on employee_name = employee.name"));

        // Repeat the join again, but in Spark API way
        printDataset(department.join(employee, col("employee_name").equalTo(employee.col("name"))));
    }
}
