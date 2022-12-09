package sparkdemo;

import java.util.ArrayList;
import java.util.List;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
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
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.functions;
import org.apache.log4j.Logger;
import org.apache.log4j.Level;
import org.apache.spark.sql.expressions.WindowSpec;
import java.io.*;  
import java.util.Scanner;  


public class category12count {

	public static void main(String[] args) throws IOException {
	
		
		if(args[0].equals("1") || args[0].equals("2"))
		ques1();
		
		if(args[0].equals("3"))
	        ques3();
		
		if(args[0].equals("4"))
		ques4();
		
		else
		System.out.println("Incorrect Argument");
		}  
	
	 static void  ques4()
	  {
		  
			String inputPath="history_english.json";  //current workspace directory
			String outputPath="output4";       //current workspace directory
			

			Logger.getLogger("org").setLevel(Level.OFF);
			Logger.getLogger("akka").setLevel(Level.OFF);

			StructType obj = new StructType();
			obj = obj.add("date", DataTypes.IntegerType, true); // false => not nullable
			obj = obj.add("category2", DataTypes.StringType, true); // false => not nullable
			ExpressionEncoder<Row> tupleRowEncoder = RowEncoder.apply(obj);

			SparkSession sparkSession = SparkSession.builder()
					.appName("Entity Count")		//Name of application
					.master("local")								//Run the application on local node
					.config("spark.sql.shuffle.partitions","2")		//Number of partitions
					.getOrCreate();			
				
				Dataset<Row> inputDataset=sparkSession.read().option("multiLine", true).json(inputPath);
				Dataset<Row> ID = inputDataset.filter(inputDataset.col("granularity").contains("year")); 
			
				Dataset<Row> ds = ID.map(new MapFunction<Row,Row>(){
					public Row call(Row row) throws Exception{
						String e_date =((String)row.getAs("date"));
						int index=0;
						  if(e_date.contains("/"))
						    {
						        index=e_date.indexOf('/');
						        e_date=e_date.substring(0,index);
						    }
						    
						String count="1";
						
						return RowFactory.create(Integer.parseInt(e_date),count);
					}
				}, tupleRowEncoder);
				
				
				Dataset<Row> count_temp = ds.groupBy("date").count().alias("count");
				
				WindowSpec w = org.apache.spark.sql.expressions.Window.orderBy(functions.col("count").desc());
				Dataset<Row> count = count_temp.withColumn("Rank", functions.rank().over(w));
	
				count.show((int)count.count());
				count.toJavaRDD().saveAsTextFile(outputPath);
				
	}
		 
	 
	
	 static void  ques3()
	  
	  {
		 
			String inputPath="history_english.json"; //current workspace directory
			String outputPath="output3";  //current workspace directory
			
			Logger.getLogger("org").setLevel(Level.OFF);
			Logger.getLogger("akka").setLevel(Level.OFF);

			StructType obj = new StructType();
			obj = obj.add("date", DataTypes.IntegerType, true); 
			obj = obj.add("category2", DataTypes.StringType, true);
			ExpressionEncoder<Row> tupleRowEncoder = RowEncoder.apply(obj);

			SparkSession sparkSession = SparkSession.builder()
					.appName("Entity Count")		//Name of application
					.master("local")								//Run the application on local node
					.config("spark.sql.shuffle.partitions","2")		//Number of partitions
					.getOrCreate();
			
			 
			Dataset<Row> inputDataset=sparkSession.read().option("multiLine", true).json(inputPath);
			Dataset<Row> ID = inputDataset.filter(inputDataset.col("granularity").contains("year")); 
				
				
			Dataset<Row> ds = ID.map(new MapFunction<Row,Row>(){
					public Row call(Row row) throws Exception{
						String e_date =((String)row.getAs("date"));
						int index=0;
						  if(e_date.contains("/"))
						    {
						        index=e_date.indexOf('/');
						        e_date=e_date.substring(0,index);
						    }
						    
						String count="1";
						
						return RowFactory.create(Integer.parseInt(e_date),count);
					}
				}, tupleRowEncoder);
				
				
			Dataset<Row> count = ds.groupBy("date").count().alias("count");	
				
			count.sort("date").show((int)count.count());
		    count.sort("date").toJavaRDD().saveAsTextFile(outputPath);
			
	}
		     
	 
	  static void  ques1() 
	  {
		  
			String inputPath="history_english.json";    //current workspace directory
			String outputPath="output";     //current workspace directory
			

			Logger.getLogger("org").setLevel(Level.OFF);
			Logger.getLogger("akka").setLevel(Level.OFF);

			StructType obj = new StructType();
			obj = obj.add("category1", DataTypes.StringType, true); 
			obj = obj.add("category2", DataTypes.StringType, true); 
			ExpressionEncoder<Row> tupleRowEncoder = RowEncoder.apply(obj);

			SparkSession sparkSession = SparkSession.builder()
					.appName("Entity Count")		//Name of application
					.master("local")								//Run the application on local node
					.config("spark.sql.shuffle.partitions","2")		//Number of partitions
					.getOrCreate();


			Dataset<Row> inputDataset=sparkSession.read().option("multiLine", true).json(inputPath);
			
			Dataset<Row> count=null;
			int flag=1;
			
			FileInputStream fis = null;
				
			try {
				fis = new FileInputStream("entities.txt");    //current workspace directory
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}       
			Scanner sc=new Scanner(fis);   
			
			
			while(sc.hasNextLine())  
			{  
			String str=sc.nextLine();     
			
			Dataset<Row> ID = inputDataset.filter(inputDataset.col("description").contains(str)); 
			
			Dataset<Row> ds = ID.map(new MapFunction<Row,Row>(){
				public Row call(Row row) throws Exception{
					String category1 =((String)row.getAs("category1"));
					String category2 = ((String)row.getAs("category2"));
					return RowFactory.create(category1,category2);
				}
			}, tupleRowEncoder);
			
			
			Dataset<Row> dfs = ds.filter(ds.col("category1"). isNotNull());
			Dataset<Row> dfs2 = dfs.filter(dfs.col("category2"). isNotNull());
			
			Dataset<Row> ID2 = dfs2.withColumn("entity",functions.lit(str));  //adding entity column 
			
			Dataset<Row> count_temp = ID2.groupBy("entity","category1","category2").count().alias("count");
			
		
			if(flag==1)
			{ 
				count=count_temp;
				flag=0;            
			}
			else
			count=count.union(count_temp);
			
			
			}
			count.show((int)count.count());
			sc.close();    
		    count.toJavaRDD().saveAsTextFile(outputPath);
		  
	  }
	}