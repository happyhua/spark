package spark.java;

import org.apache.spark.sql.Dataset;
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
        Dataset<Employee> employee = createEmployeeDataset(spark);

        // Print the schema and show employee data table, up to 20 rows.
        printDataset(employee);

        // Filter employee Dataset with a condition on age column
        System.out.println("select * from employee where age <= 50, in spark api way.");
        employee.filter(col("age").leq(lit(50))).show();

        // Filter age column again, but in spark sql way
        // You need to create a temp view to use the Dataset in sql!
        employee.createOrReplaceTempView("employee");
        System.out.println("select * from employee where age <= 50, in spark sql way.");
        spark.sql("select * from employee where age <= 50").show();

        // Group employee Dataset by gender column and count the employee size of each gender
        System.out.println("select gender, count(*) from employee group by gender, in spark api way");
        employee.groupBy("gender").count().show();

        // Group gender again, but in spark sql way
        System.out.println("select gender, count(*) from employee group by gender, in spark sql way");
        spark.sql("select gender, count(*) from employee group by gender").show();

        // Create an embedded department Dataset
        Dataset<Row> department = createDepartmentDataset(spark);
        printDataset(department);

        // Do an inner join between department Dataset and employee Dataset, based on employee_name
        printDataset(department.join(employee, col("employee_name").equalTo(employee.col("name"))));

        // Repeat the join again, but in spark sql way
        // You need to create a temp view to use the Dataset in sql!
        department.createOrReplaceTempView("department");
        printDataset(spark.sql("select * from department join employee on employee_name = employee.name"));

        // Create an embedded employee Dataset and write it to a storage
        createAndWriteEmployeeDataset(spark);
    }
}
