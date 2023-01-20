import org.apache.spark.sql.SparkSession;

public class HelloWorld {
  public static void main(String[] args) {
    SparkSession spark = SparkSession.builder().appName("Simple Application").getOrCreate();
    
    System.out.println("Hello, from EMR Serverless!");

    spark.stop();
  }
}