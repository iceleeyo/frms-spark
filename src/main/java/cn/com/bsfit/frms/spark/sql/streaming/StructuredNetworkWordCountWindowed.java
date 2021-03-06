package cn.com.bsfit.frms.spark.sql.streaming;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.StreamingQuery;
import scala.Tuple2;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class StructuredNetworkWordCountWindowed {

	public static void main(String[] args) {
		if (args.length < 3) {
			System.err.println("Usage: JavaStructuredNetworkWordCountWindowed <hostname> <port>"
					+ " <window duration in seconds> [<slide duration in seconds>]");
			System.exit(1);
		}

		String host = args[0];
		int port = Integer.parseInt(args[1]);
		int windowSize = Integer.parseInt(args[2]);
		int slideSize = (args.length == 3) ? windowSize : Integer.parseInt(args[3]);
		if (slideSize > windowSize) {
			System.err.println("<slide duration> must be less than or equal to <window duration>");
		}
		String windowDuration = windowSize + " seconds";
		String slideDuration = slideSize + " seconds";

		SparkSession spark = SparkSession.builder().appName("JavaStructuredNetworkWordCountWindowed").getOrCreate();

		Dataset<Tuple2<String, Timestamp>> lines = spark.readStream().format("socket").option("host", host)
				.option("port", port).option("includeTimestamp", true).load()
				.as(Encoders.tuple(Encoders.STRING(), Encoders.TIMESTAMP()));

		Dataset<Row> words = lines.flatMap(new FlatMapFunction<Tuple2<String, Timestamp>, Tuple2<String, Timestamp>>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Iterator<Tuple2<String, Timestamp>> call(Tuple2<String, Timestamp> t) {
				List<Tuple2<String, Timestamp>> result = new ArrayList<>();
				for (String word : t._1.split(" ")) {
					result.add(new Tuple2<>(word, t._2));
				}
				return result.iterator();
			}
		}, Encoders.tuple(Encoders.STRING(), Encoders.TIMESTAMP())).toDF("word", "timestamp");

		Dataset<Row> windowedCounts = words
				.groupBy(functions.window(words.col("timestamp"), windowDuration, slideDuration), words.col("word"))
				.count().orderBy("window");

		StreamingQuery query = windowedCounts.writeStream().outputMode("complete").format("console")
				.option("truncate", "false").start();

		query.awaitTermination();
	}
}
