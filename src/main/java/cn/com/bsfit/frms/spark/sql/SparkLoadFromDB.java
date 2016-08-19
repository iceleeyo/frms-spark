package cn.com.bsfit.frms.spark.sql;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SparkLoadFromDB {

	private static final String MYSQL_USERNAME = "expertuser";
	private static final String MYSQL_PWD = "expertuser123";
	private static final String MYSQL_DRIVER = "com.mysql.jdbc.Driver";
	private static final String MYSQL_CONNECTION_URL2 = "jdbc:mysql://localhost:3306/employees";
	private static final Logger LOGGER = LoggerFactory.getLogger(SparkLoadFromDB.class);
	private static final SparkSession sparkSession = SparkSession.builder().master("local[*]").appName("Spark2JdbcDs")
			.getOrCreate();
	private static final String MYSQL_CONNECTION_URL = "jdbc:mysql://localhost:3306/employees?user=" + MYSQL_USERNAME
			+ "&password=" + MYSQL_PWD;

	private static final JavaSparkContext sc = new JavaSparkContext(
			new SparkConf().setAppName("SparkJdbcDs").setMaster("local[*]"));

	private static final SQLContext sqlContext = SparkSession.builder().sparkContext(sc.sc()).getOrCreate().sqlContext();

	public static void main(String[] args) {
		
	}

	public static void spark_2_0_0_load_from_db() {
		final Properties connectionProperties = new Properties();
		connectionProperties.put("user", MYSQL_USERNAME);
		connectionProperties.put("password", MYSQL_PWD);

		final String dbTable = "(select emp_no, concat_ws(' ', first_name, last_name) as full_name from employees) as employees_name";

		Dataset<Row> jdbcDF = sparkSession.read().jdbc(MYSQL_CONNECTION_URL2, dbTable, "emp_no", 10001, 499999, 10,
				connectionProperties);

		List<Row> employeeFullNameRows = jdbcDF.collectAsList();

		for (Row employeeFullNameRow : employeeFullNameRows) {
			LOGGER.info(employeeFullNameRow.toString());
		}
	}

	public static void spark_load_from_db() {
		Map<String, String> options = new HashMap<>();
		options.put("driver", MYSQL_DRIVER);
		options.put("url", MYSQL_CONNECTION_URL);
		options.put("dbtable",
				"(select emp_no, concat_ws(' ', first_name, last_name) as full_name from employees) as employees_name");
		options.put("partitionColumn", "emp_no");
		options.put("lowerBound", "10001");
		options.put("upperBound", "499999");
		options.put("numPartitions", "10");

		Dataset<Row> jdbcDF = sqlContext.read().format("jdbc").options(options).load();

		List<Row> employeeFullNameRows = jdbcDF.collectAsList();

		for (Row employeeFullNameRow : employeeFullNameRows) {
			LOGGER.info(employeeFullNameRow.toString());
		}
	}
}
