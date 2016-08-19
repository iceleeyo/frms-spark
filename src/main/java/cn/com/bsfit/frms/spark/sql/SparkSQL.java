package cn.com.bsfit.frms.spark.sql;

import static org.apache.spark.sql.functions.col;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import cn.com.bsfit.frms.spark.pojo.Person;
import cn.com.bsfit.frms.spark.utils.Constants;

@Component
public class SparkSQL {

	@Autowired
	private transient SparkSession sparkSession;
	
	public void executeSparkSQL() {
		runBasicDataFrameExample(sparkSession);
		runDatasetCreationExample(sparkSession);
		runInferSchemaExample(sparkSession);
		runProgrammaticSchemaExample(sparkSession);
	}

	private static void runBasicDataFrameExample(final SparkSession spark) {
//		Properties properties = new Properties();
//		properties.put("user", "lkl_ds_test");
//		properties.put("password", "bangsun");
//		properties.put("driver", Constants.ORACLE_DRIVER_CLASS);
//		
//		
//		final Dataset<Row> sqlDf = spark.read().jdbc("jdbc:oracle:thin:@10.100.1.20:1521:db3", "OFFLINE_PAY_ORDER", properties);
//		long count = sqlDf.count();
				
		
		Map<String, String> options = new HashMap<>();
		options.put("url", Constants.JDBC_URL);
		options.put("dbtable", "LKL_DS_TEST.OFFLINE_PAY_ORDER");
		options.put("user", "lkl_ds_test");
		options.put("password", "bangsun");
		options.put("driver", Constants.ORACLE_DRIVER_CLASS);

		
		
		Dataset<Row> jdbcDF = spark.read().format("jdbc").options(options).load();
		
		Dataset<Row> aaa = jdbcDF.groupBy("TRANS_PRC_CODE").count();
		aaa.show();
		
		spark.sql("");
		
		
		final Dataset<Row> df = spark.read().json("src/main/resources/people.json");

		df.show();

		df.printSchema();

		df.select("name").show();

		df.select(col("name"), col("age").plus(1)).show();
		df.filter(col("age").gt(21)).show();

		df.groupBy("age").count().show();
		df.createOrReplaceTempView("people");

		Dataset<Row> sqlDF = spark.sql("SELECT * FROM people");
		sqlDF.show();
	}

	private static void runDatasetCreationExample(SparkSession spark) {
		Person person = new Person();
		person.setName("Andy");
		person.setAge(32);
		Encoder<Person> personEncoder = Encoders.bean(Person.class);
		Dataset<Person> javaBeanDS = spark.createDataset(Collections.singletonList(person), personEncoder);
		javaBeanDS.show();

		Encoder<Integer> integerEncoder = Encoders.INT();
		Dataset<Integer> primitiveDS = spark.createDataset(Arrays.asList(1, 2, 3), integerEncoder);
		Dataset<Integer> transformedDS = primitiveDS.map(new MapFunction<Integer, Integer>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Integer call(Integer value) throws Exception {
				return value + 1;
			}
		}, integerEncoder);
		transformedDS.collect();

		String path = "src/main/resources/people.json";
		Dataset<Person> peopleDS = spark.read().json(path).as(personEncoder);
		peopleDS.show();
	}

	private static void runInferSchemaExample(SparkSession spark) {
		JavaRDD<Person> peopleRDD = spark.read().textFile("src/main/resources/people.txt").javaRDD()
				.map(new Function<String, Person>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Person call(String line) throws Exception {
						String[] parts = line.split(",");
						Person person = new Person();
						person.setName(parts[0]);
						person.setAge(Integer.parseInt(parts[1].trim()));
						return person;
					}
				});
		Dataset<Row> peopleDF = spark.createDataFrame(peopleRDD, Person.class);
		peopleDF.createOrReplaceTempView("people");
		Dataset<Row> teenagersDF = spark.sql("SELECT name FROM people WHERE age BETWEEN 13 AND 19");
		Encoder<String> stringEncoder = Encoders.STRING();
		Dataset<String> teenagerNamesByIndexDF = teenagersDF.map(new MapFunction<Row, String>() {
			private static final long serialVersionUID = 1L;

			@Override
			public String call(Row row) throws Exception {
				return "Name: " + row.getString(0);
			}
		}, stringEncoder);
		teenagerNamesByIndexDF.show();
		Dataset<String> teenagerNamesByFieldDF = teenagersDF.map(new MapFunction<Row, String>() {
			private static final long serialVersionUID = 1L;

			@Override
			public String call(Row row) throws Exception {
				return "Name: " + row.<String> getAs("name");
			}
		}, stringEncoder);
		teenagerNamesByFieldDF.show();
	}

	private static void runProgrammaticSchemaExample(SparkSession spark) {
		JavaRDD<String> peopleRDD = spark.sparkContext().textFile("src/main/resources/people.txt", 1).toJavaRDD();
		String schemaString = "name age";

		List<StructField> fields = new ArrayList<>();
		for (String fieldName : schemaString.split(" ")) {
			StructField field = DataTypes.createStructField(fieldName, DataTypes.StringType, true);
			fields.add(field);
		}
		StructType schema = DataTypes.createStructType(fields);

		JavaRDD<Row> rowRDD = peopleRDD.map(new Function<String, Row>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Row call(String record) throws Exception {
				String[] attributes = record.split(",");
				return RowFactory.create(attributes[0], attributes[1].trim());
			}
		});
		Dataset<Row> peopleDataFrame = spark.createDataFrame(rowRDD, schema);
		peopleDataFrame.createOrReplaceTempView("people");
		Dataset<Row> results = spark.sql("SELECT name FROM people");
		Dataset<String> namesDS = results.map(new MapFunction<Row, String>() {
			private static final long serialVersionUID = 1L;

			@Override
			public String call(Row row) throws Exception {
				return "Name: " + row.getString(0);
			}
		}, Encoders.STRING());
		namesDS.show();
	}
}
