package com.stuparm.learningspark;

import com.stuparm.learningspark.util.Book;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Example05 implements Executable{


    @Override
    public void exec(ExecContext context) {

        SparkSession spark = SparkSession.builder().
                appName("books conversion").
                master("local").
                getOrCreate();

        String filename = "data/books.csv";
        Dataset<Row> df = spark.read().
                format("csv").
                option("inferSchema","true").
                option("header","true").
                load(filename);

        df.show(5,17);


        Dataset<Book> bookDs = df.map(new BookMapper(), Encoders.bean(Book.class));
        bookDs.show(5);



        Dataset<Row> df2 = bookDs.toDF();
        df2.printSchema();

    }
}

class BookMapper implements MapFunction<Row, Book> {
    private static final long serialVersionUID = -2l;

    @Override
    public Book call(Row value) throws Exception {
        Book b = new Book();
        b.setId(value.getAs("id"));
        b.setAuthorId(value.getAs("authorId"));
        b.setTitle(value.getAs("title"));
        b.setLink(value.getAs("link"));

        String releaseDateStr = value.getAs("releaseDate");
        if (releaseDateStr != null) {
            SimpleDateFormat formatter = new SimpleDateFormat("M/dd/yy");
            Date releaseDate = formatter.parse(value.getAs("releaseDate"));
            b.setReleaseDate(releaseDate);
        }

        return b;
    }

}

