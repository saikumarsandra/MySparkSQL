package com.virtualpairprogrammers;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class BigLog {

	public static void main(String [] args) {
		
		 System.setProperty("hadoop.home.dir", "c:/hadoop"); 
			
		   Logger.getLogger("org.apache").setLevel( Level.WARN);
		   
		   SparkSession spark = SparkSession
				                            .builder()
				                            .appName("my sql spark data")
				                            .master("local[*]")
				                            .config("spark.sql.warehouse.dir","file:///c:/tmp")
				                            .getOrCreate();
		// create a data set reading the csv file 
		  Dataset<Row> dataset = spark.read().option("header", true)
				                      .csv("src/main/resources/subtitles/biglog.txt");
		 
		  
		  dataset.createOrReplaceTempView("logging_table");
		  Dataset<Row> resultset = spark.sql("select level, date_format(datetime,'MMMM') as month , count(1) as totals from logging_table group by level,month");

		   resultset.show();
		   
		   resultset.createOrReplaceTempView("results_table");
  Dataset<Row> totals = spark.sql("select sum(total) from results_table");
  totals.show();
		   spark.close();
		
	}
}
