package cn.com.bsfit.frms.spark.sql;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import cn.com.bsfit.frms.spark.pojo.Cube;
import cn.com.bsfit.frms.spark.pojo.Square;

@Component
public class SQLDataSource {

	@Autowired
	private transient SparkSession sparkSession;

	public void executeSparkSQL() {
		runBasicDataSourceExample(sparkSession);
		runBasicParquetExample(sparkSession);
		runParquetSchemaMergingExample(sparkSession);
		runJsonDatasetExample(sparkSession);
	}

	private static void runBasicDataSourceExample(SparkSession spark) {
		Dataset<Row> usersDF = spark.read().load("src/main/resources/users.parquet");
		usersDF.select("name", "favorite_color").write().save("namesAndFavColors.parquet");
		Dataset<Row> peopleDF = spark.read().format("json").load("src/main/resources/people.json");
		peopleDF.select("name", "age").write().format("parquet").save("namesAndAges.parquet");
		Dataset<Row> sqlDF = spark.sql("SELECT * FROM parquet.`src/main/resources/users.parquet`");
		sqlDF.show();
	}

	private static void runBasicParquetExample(SparkSession spark) {
		Dataset<Row> peopleDF = spark.read().json("src/main/resources/people.json");
		peopleDF.write().parquet("people.parquet");
		Dataset<Row> parquetFileDF = spark.read().parquet("people.parquet");
		parquetFileDF.createOrReplaceTempView("parquetFile");
		Dataset<Row> namesDF = spark.sql("SELECT name FROM parquetFile WHERE age BETWEEN 13 AND 19");
		Dataset<String> namesDS = namesDF.map(new MapFunction<Row, String>() {
			private static final long serialVersionUID = 1L;
			public String call(Row row) {
				return "Name: " + row.getString(0);
			}
		}, Encoders.STRING());
		namesDS.show();
	}

	private static void runParquetSchemaMergingExample(SparkSession spark) {
		List<Square> squares = new ArrayList<>();
		for (int value = 1; value <= 5; value++) {
			Square square = new Square();
			square.setValue(value);
			square.setSquare(value * value);
			squares.add(square);
		}

		Dataset<Row> squaresDF = spark.createDataFrame(squares, Square.class);
		squaresDF.write().parquet("data/test_table/key=1");

		List<Cube> cubes = new ArrayList<>();
		for (int value = 6; value <= 10; value++) {
			Cube cube = new Cube();
			cube.setValue(value);
			cube.setCube(value * value * value);
			cubes.add(cube);
		}

		Dataset<Row> cubesDF = spark.createDataFrame(cubes, Cube.class);
		cubesDF.write().parquet("data/test_table/key=2");

		Dataset<Row> mergedDF = spark.read().option("mergeSchema", true).parquet("data/test_table");
		mergedDF.printSchema();
	}

	private static void runJsonDatasetExample(SparkSession spark) {
		Dataset<Row> people = spark.read().json("src/main/resources/people.json");

		// The inferred schema can be visualized using the printSchema() method
		people.printSchema();
		// root
		// |-- age: long (nullable = true)
		// |-- name: string (nullable = true)

		// Creates a temporary view using the DataFrame
		people.createOrReplaceTempView("people");

		// SQL statements can be run by using the sql methods provided by spark
		Dataset<Row> namesDF = spark.sql("SELECT name FROM people WHERE age BETWEEN 13 AND 19");
		namesDF.show();
		// +------+
		// | name|
		// +------+
		// |Justin|
		// +------+
		// $example off:json_dataset$
	}
}
