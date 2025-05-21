package com.stuparm.learningspark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;

public class Example06 implements Executable {


    @Override
    public void exec(ExecContext context) {

        // 1
        long t0 = System.currentTimeMillis();
        SparkSession spark = SparkSession.builder().
                appName("health").
                master("local").
                getOrCreate();
        long t1 = System.currentTimeMillis();
        System.out.println("1. Creating a session ........... " + (t1 - t0));


        // 2
        Dataset<Row> df = spark.read().
                format("csv").
                option("header","true").
                load("data/health.csv");
        Dataset<Row> initialDf = df;
        long t2 = System.currentTimeMillis();
        System.out.println("2. Loading initial dataset . .... " + (t2 - t1));


        // 3
//        for (int i = 0; i < 6; i++) {
//            df = df.union(initialDf);
//        }
        long t3 = System.currentTimeMillis();
        System.out.println("3. Building full dataset ........ " + (t3 - t2));


        // 4
        df = df.withColumnRenamed("Lower Confidence Limit","lcl").
                withColumnRenamed("Upper Confidence Limit", "ucl");
        long t4 = System.currentTimeMillis();
        System.out.println("4. Clean-up ..................... " + (t4 - t3));


        // 5
        df = df.withColumn("avg", expr("(lcl+ucl)/2"))
                .withColumn("lcl2", df.col("lcl"))
                .withColumn("ucl2", df.col("ucl"));;
        long t5 = System.currentTimeMillis();
        System.out.println("5. Transformations ..................... " + (t5 - t4));



        df.explain(true);
        long t6 = System.currentTimeMillis();
        System.out.println("6. Collect ............................. " + (t6 - t5));

    }
}
