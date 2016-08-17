package cn.com.bsfit.frms.spark.sql.streaming;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;

public class StructuredNetworkWordCount {
	public static void main(String[] args) throws Exception {
		if (args.length < 2) {
			System.err.println("Usage: JavaStructuredNetworkWordCount <hostname> <port>");
			System.exit(1);
		}

		String host = args[0];
		int port = Integer.parseInt(args[1]);

		SparkSession spark = SparkSession.builder().appName("JavaStructuredNetworkWordCount").getOrCreate();

		Dataset<String> lines = spark.readStream().format("socket").option("host", host).option("port", port).load()
				.as(Encoders.STRING());

		Dataset<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
			private static final long serialVersionUID = 1L;
			@Override
			public Iterator<String> call(String x) {
				return Arrays.asList(x.split(" ")).iterator();
			}
		}, Encoders.STRING());
		
		Dataset<Row> wordCounts = words.groupBy("value").count();

		StreamingQuery query = wordCounts.writeStream().outputMode("complete").format("console").start();

		query.awaitTermination();
	}
}
