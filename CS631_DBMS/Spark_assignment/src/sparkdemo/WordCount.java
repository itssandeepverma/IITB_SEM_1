package sparkdemo;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.catalyst.expressions.DenseRank;
import org.apache.spark.sql.types.BinaryType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.functions;
import org.apache.log4j.Logger;
import org.apache.log4j.Level;
import org.apache.spark.sql.expressions.WindowSpec;


/**
 * This class uses Dataset APIs of spark to count number of events per category1-category2
 * */

public class WordCount {

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		// Input dir - should contain all input json files
		String inputPath="history_english.json"; //Use absolute paths
		// Ouput dir - this directory will be created by spark. Delete this directory between each run
		String outputPath="output";   //Use absolute paths

		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);

		StructType obj = new StructType();
		obj = obj.add("category1", DataTypes.StringType, true); // false => not nullable
		obj = obj.add("category2", DataTypes.StringType, true); // false => not nullable
		ExpressionEncoder<Row> tupleRowEncoder = RowEncoder.apply(obj);

		SparkSession sparkSession = SparkSession.builder()
				.appName("Entity Count")		//Name of application
				.master("local")								//Run the application on local node
				.config("spark.sql.shuffle.partitions","2")		//Number of partitions
				.getOrCreate();

		// Read multi-line JSON from input files to dataset
		Dataset<Row> inputDataset=sparkSession.read().option("multiLine", true).json(inputPath);;
		StructType structure = new StructType(new StructField[]{
				new StructField("entity", DataTypes.StringType, true, Metadata.empty()),
				new StructField("category1", DataTypes.StringType, true, Metadata.empty()),
				new StructField("category2", DataTypes.StringType, true, Metadata.empty()),
				new StructField("count", DataTypes.StringType, true, Metadata.empty())
		});
		List<Row> rows = new ArrayList<Row>();
		Dataset<Row> global = sparkSession.createDataFrame(rows, structure);
		//inputDataset.show();
		Scanner clInput = new Scanner(System.in);
		int a = clInput.nextInt();
		
		if(a==1) {
			//the file to be opened for reading  
			FileInputStream fis=new FileInputStream("entities.txt");       
			Scanner sc=new Scanner(fis);    //file to be scanned 
			//returns true if there is another line to read  
			while(sc.hasNextLine())  
			{  
				Dataset<Row> copyDataset = inputDataset;
				Dataset<Row> local = CreateDataset(copyDataset,sc.nextLine());//returns the line that was skipped  
				global = global.union(local);
			}  
			sc.close();
			global.show();
		}
		else if(a==2) {
			global = CountInstances(inputDataset);
			global.show();
		}
		else {
			global = CountInstances(inputDataset);
			global = CalculateRank(global);
			global.show();
		}
		
		// Ouputs the result to the folder "outputPath"
		global.toJavaRDD().saveAsTextFile(outputPath);
	}
	
	public static Dataset<Row> CreateDataset(Dataset<Row> ds, String entity) {
		Dataset<Row> finalDs = ds.filter(ds.col("category1"). isNotNull());
		// Delete all rows with null values in category2 column
		finalDs = finalDs.filter(finalDs.col("category2"). isNotNull());
		finalDs = finalDs.filter(finalDs.col("description").contains(entity));
		finalDs = finalDs.withColumn("entity", functions.lit(entity));
		finalDs = finalDs.drop("date", "granuality", "lang");
		finalDs = finalDs.groupBy("entity","category1","category2").count().alias("count");
		return finalDs;
	}
	
	public static Dataset<Row> CountInstances(Dataset<Row> ds){
		Dataset<Row> finalDs = ds.filter(ds.col("date").isNotNull());
		finalDs = finalDs.filter(finalDs.col("granularity"). isNotNull());
		finalDs = finalDs.filter(finalDs.col("granularity").contains("year"));
		finalDs = finalDs.groupBy("date").count().alias("count");
		return finalDs;
	}
	
	public static Dataset<Row> CalculateRank(Dataset<Row> ds){
		Dataset<Row> finalDs = ds.withColumn("rank", functions.rank().over(Window.orderBy(org.apache.spark.sql.functions.col("count").desc())));
		return finalDs;
	}

}