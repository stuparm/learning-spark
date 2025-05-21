package com.stuparm.learningspark;

import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.lit;

import java.util.Properties;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

public class Example02 implements Executable{


    @Override
    public void exec(ExecContext context) {
        SparkSession spark = SparkSession.builder()
                .appName("CSV to DB")
                .master("local")
                .getOrCreate();


        Dataset<Row> df = spark.read()
                .format("csv")
                .option("header", "true")
                .load("data/authors.csv");


        df = df.withColumn(
                "name",
                concat(df.col("lname"), lit(", "), df.col("fname")));



        String dbConnectionUrl = "jdbc:postgresql://localhost:5551/spark1";

        Properties prop = new Properties();
        prop.setProperty("driver", "org.postgresql.Driver");
        prop.setProperty("user", "postgres");
        prop.setProperty("password", "password");


        df.write()
                .mode(SaveMode.Overwrite)
                .jdbc(dbConnectionUrl, "example2", prop);

        System.out.println("Process complete");
    }

}