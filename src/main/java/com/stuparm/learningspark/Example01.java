package com.stuparm.learningspark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Example01 implements Executable {

    public void exec(ExecContext context) {
        SparkSession spark = SparkSession.builder()
                .appName("CSV to Dataset")
                .master("local")
                .getOrCreate();

        // Reads a CSV file with header, called books.csv, stores it in a
        // dataframe
        Dataset<Row> df = spark.read().format("csv")
                .option("header", "true")
                .load("data/books.csv");


        df = df.withColumn("authorId3",df.col("authorId").plus("2").as("authorId2"));
        df.printSchema(5);

        df.show(5);
    }


}
