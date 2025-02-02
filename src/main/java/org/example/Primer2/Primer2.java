package org.example.Primer2;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.*;

import javax.xml.crypto.Data;
import java.io.*;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.*;

public class Primer2 {
    public static void primer22(SparkSession spark){

Dataset<Row> customers_df = spark.read()
        .format("csv")
        .option("header", true)
        .csv("C:/Users/koshkinas/Desktop/spark/untitled5/src/main/resources/customers.csv");

        Dataset<Row> products_df = spark.read()
                .format("csv")
                .option("header", true)
                .csv("C:/Users/koshkinas/Desktop/spark/untitled5/src/main/resources/products.csv");

        Dataset<Row> sales_df = spark.read()
                .format("csv")
                .option("header", true)
                .csv("C:/Users/koshkinas/Desktop/spark/untitled5/src/main/resources/sales_data.csv");

        Dataset<Row> products_df1 = products_df.withColumn("price", col("price").cast(DataTypes.DoubleType));
        Dataset<Row> sales_df1 = sales_df.withColumn("amount", col("amount").cast(DataTypes.DoubleType))
                .withColumn("transaction_date", col("transaction_date").cast(DataTypes.DateType));
    //    products_df1.printSchema();
  //      sales_df1.printSchema();


// 2.	Объединение таблиц:
        Dataset<Row> s_p_df1  = sales_df1.join(products_df1, "transaction_id", "inner")
                .join(customers_df, "customer_id", "inner");
   //     s_p_df1.show();
        s_p_df1.cache();

//3.	Анализ транзакций:
        WindowSpec windowSpec1 = Window.partitionBy("customer_id").orderBy(col("transaction_date").asc());
        WindowSpec windowSpec2 = Window.partitionBy("transaction_id").orderBy(col("price").asc());
        WindowSpec windowSpec3 = Window.partitionBy("customer_id").orderBy(col("transaction_date").asc());
        WindowSpec windowSpec4 = Window.partitionBy("region");

        Dataset<Row> s_p_df_2 = s_p_df1.withColumn("rank1", rank().over(windowSpec1))
                .withColumn("rank2", rank().over(windowSpec2))
                .withColumn("summ_tran", sum("amount").over(windowSpec3))
                .withColumn("avg_tran_region", avg("amount").over(windowSpec4));
     //   s_p_df_2.show();

//4.	Анализ по количеству продуктов:
                String row1  = s_p_df_2.groupBy("product_name")
                .agg(sum("amount").as("sum_amount")).orderBy(col("sum_amount").desc())
                        .first().toString().split(",")[0];
            String row2 = row1.substring(1, row1.length());
       // System.out.println(row2);

//5.	Периодические изменения в покупках:
        WindowSpec windowSpec5 = Window.partitionBy("customer_id").orderBy(col("transaction_date").asc());

        Dataset<Row> s_p_df3 = s_p_df1.withColumn("difference_tran", col("amount").minus(lag("amount", 1,  null).over(windowSpec5)))
                .where(col("difference_tran").isNotNull().and(col("difference_tran").gt(50)));
            //    s_p_df3.show();

//6.	Группировка по регионам:
            Dataset<Row> s_p_df4 = s_p_df1.groupBy("region").agg(sum("amount").as("sum_amount"));
            Dataset<Row> s_p_df5 = s_p_df1.groupBy("region", "customer_id").agg(round(avg("amount"), 2).as("avg_purchase"));
            Dataset<Row> s_p_df6 = s_p_df1.groupBy("region").agg(round(avg("amount"), 2).as("avg_purchase"));

         //   s_p_df4.show();
         //   s_p_df5.show();
         //   s_p_df6.show();

//7.	Сохранение результатов:
    List<String> list = s_p_df6.collectAsList().stream()
            .map(r->r.toString())
            .map(str->str.substring(1, str.length() - 1))
            .toList();
   // list.forEach(System.out::println);

    list.forEach(x -> {
        String str_name = x.split(",")[0];
        String path = "C:/Users/koshkinas/Desktop/spark/untitled5/src/main/resources/" + str_name + ".csv";
        String string1 = "region,avg_amount";
        File file_new = new File(path);
        try {
            file_new.createNewFile();
            FileWriter fileWriter = new FileWriter(path, true);
            fileWriter.write(string1);
            fileWriter.write("\n");
            fileWriter.write(x);
            fileWriter.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    });

    }
}


