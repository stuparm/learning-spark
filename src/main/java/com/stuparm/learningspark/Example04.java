package com.stuparm.learningspark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.List;

public class Example04 implements Executable{

    @Override
    public void exec(ExecContext context) {

        SparkSession spark = SparkSession.builder().
                appName("array of Dataset<String>").
                master("local").getOrCreate();

        List<String> data = Arrays.asList("Jean", "Liz", "Pierre", "Lauric");
        Dataset<String> ds = spark.createDataset(data, Encoders.STRING());
        ds.show();
        ds.printSchema();

    }
}
