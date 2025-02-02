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
import org.example.Primer1.Primer1;
import org.example.Primer2.Primer2;

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

     //   Primer1.primer11(spark);
        Primer2.primer22(spark);
    }
}