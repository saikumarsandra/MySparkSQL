package com.virtualpairprogrammers;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkSql {

	public static void main(String[] args) {
		  
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
				                       .csv("src/main/resources/exams/students.csv");
		   
		   dataset.show();
		 long number=  dataset.count();
		 
		 System.out.println("thier are"+" "+number+" "+"of records in the students.csv");
		   
		   spark.close();
	}

}
