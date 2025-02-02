package org.example;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;

import java.time.LocalDate;
import java.util.Date;

import static org.apache.spark.sql.functions.*;

public class Main {
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("Simple Application")
                .master("local[*]")
                .getOrCreate();

        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());





        Dataset<Row> df1 = spark.read()
                .option("header", true)
                .option("inferSchema", "true")
             //   .schema()
                .csv("C:/Users/koshkinas/Desktop/spark/untitled5/src/main/resources/file1.csv");

        WindowSpec windowSpec1 = Window.partitionBy("customer_id").orderBy(col("customer_id").asc(), col("transaction_date").asc());

        Dataset<Row> df2 = df1.withColumn("row_number", row_number().over(windowSpec1))
                .withColumn("running_total", sum("amount").over(windowSpec1))
                .withColumn("running_average", round(avg("amount").over(windowSpec1),2))
                .withColumn("max_transaction_amount", max("amount").over(windowSpec1));



        LocalDate date = df1.agg(max(col("transaction_date")).as("max_date")).first().getDate(0).toLocalDate();
        LocalDate date1 = date.minusDays(3);

WindowSpec windowSpec2 = Window.partitionBy("customer_id").orderBy(col("transaction_date").asc()).rowsBetween(Window.unboundedPreceding(), Window.currentRow());
        WindowSpec windowSpec3 = Window.partitionBy("customer_id").orderBy(col("transaction_date").asc());
Dataset<Row> df4 = df2.filter(col("transaction_date").between(date1, date))
                .withColumn("sko", sum("amount").over(windowSpec2));

String path2 = "C:/Users/koshkinas/Desktop/spark/untitled5/src/main/resources/file2.csv";


       Dataset<Row> df5 = df4.filter(col("row_number").leq(3));
df5.coalesce(1)
        .write()
                .option("header", true)
                                .mode("overwrite")
                                        .csv(path2);



WindowSpec windowSpec4 = Window.partitionBy("customer_id").orderBy(col("transaction_id").asc());

        Dataset<Row> df6 = df2.withColumn("lead", col("amount").minus(lead("amount", 1, 0).over(windowSpec4)));


        LocalDate date2 = date.minusDays(7);

        WindowSpec windowSpec5 = Window.partitionBy("customer_id").orderBy(col("transaction_date").asc()).rowsBetween(Window.unboundedPreceding(), Window.currentRow());
        Dataset<Row> df7 = df2.filter(col("transaction_date").between(date2, date))
                .withColumn("max_sko", max("amount").over(windowSpec2));
        df7.show();
    }
}