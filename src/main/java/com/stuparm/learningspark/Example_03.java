package com.stuparm.learningspark;

import org.apache.spark.Partition;
import org.apache.spark.internal.config.R;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.datasources.FilePartition;

import javax.xml.crypto.Data;
import java.util.Arrays;

import static org.apache.spark.sql.functions.*;

public class Example_03 implements Executable{


    @Override
    public void exec(ExecContext context) {

        SparkSession sparkSession = SparkSession.builder().appName("example3").master("local").getOrCreate();

        Dataset<Row> df = sparkSession.
                read().
                format("csv").
                option("header",true).
                load("data/Restaurants_in_Wake_County_NC.csv");

        df = df.withColumn("county", lit("Wake"))
                .withColumnRenamed("HSISID", "datasetId")
                .withColumnRenamed("NAME", "name")
                .withColumnRenamed("ADDRESS1", "address1")
                .withColumnRenamed("ADDRESS2", "address2")
                .withColumnRenamed("CITY", "city")
                .withColumnRenamed("STATE", "state")
                .withColumnRenamed("POSTALCODE", "zip")
                .withColumnRenamed("PHONENUMBER", "tel")
                .withColumnRenamed("RESTAURANTOPENDATE", "dateStart")
                .withColumnRenamed("FACILITYTYPE", "type")
                .withColumnRenamed("X", "geoX")
                .withColumnRenamed("Y", "geoY")
                .drop("OBJECTID")
                .drop("PERMITID")
                .drop("GEOCODESTATUS");


        df = df.withColumn("id", concat(
                df.col("state"), lit("_"),
                df.col("county"), lit("_"),
                df.col("datasetId")));

        df.show(5);

        df = df.repartition(4);
        Partition[] partitions = df.rdd().partitions();

        Dataset<Row> df2 = sparkSession.read().format("json").load("data/Restaurants_in_Durham_County_NC.json");



        df2 = df2.withColumn("county", lit("Durham"))
                .withColumn("datasetId", df2.col("fields.id"))
                .withColumn("name", df2.col("fields.premise_name"))
                .withColumn("address1", df2.col("fields.premise_address1"))
                .withColumn("address2", df2.col("fields.premise_address2"))
                .withColumn("city", df2.col("fields.premise_city"))
                .withColumn("state", df2.col("fields.premise_state"))
                .withColumn("zip", df2.col("fields.premise_zip"))
                .withColumn("tel", df2.col("fields.premise_phone"))
                .withColumn("dateStart", df2.col("fields.opening_date"))
                .withColumn("dateEnd", df2.col("fields.closing_date"))
                .withColumn("type",split(df2.col("fields.type_description"), " - ").getItem(1))
                .withColumn("geoX", df2.col("fields.geolocation").getItem(0))
                .withColumn("geoY", df2.col("fields.geolocation").getItem(1));

        df2 = df2.withColumn("id",
                concat(df2.col("state"), lit("_"),
                        df2.col("county"), lit("_"),
                        df2.col("datasetId")));
        System.out.println("df2: "+ df2.count());
        df2.show(5);



        Dataset<Row> df3 = df.unionByName(df2, true);
        System.out.println("df3:" + df3.count());

        df3.show(5);
    }
}
