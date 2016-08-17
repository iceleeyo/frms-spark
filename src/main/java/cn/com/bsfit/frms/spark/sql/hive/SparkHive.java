package cn.com.bsfit.frms.spark.sql.hive;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import cn.com.bsfit.frms.spark.pojo.Record;

@Component
public class SparkHive {

	@Autowired
	private transient SparkSession sparkSession;
	
	public void executeSparkHive() {
		
		sparkSession.sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING)");
		sparkSession.sql("LOAD DATA LOCAL INPATH 'src/main/resources/kv1.txt' INTO TABLE src");

		sparkSession.sql("SELECT * FROM src").show();
		
		sparkSession.sql("SELECT COUNT(*) FROM src").show();
		
		Dataset<Row> sqlDF = sparkSession.sql("SELECT key, value FROM src WHERE key < 10 ORDER BY key");

		
		Dataset<String> stringsDS = sqlDF.map(new MapFunction<Row, String>() {
			private static final long serialVersionUID = 1L;
			@Override
			public String call(Row row) throws Exception {
				return "Key: " + row.get(0) + ", Value: " + row.get(1);
			}
		}, Encoders.STRING());
		stringsDS.show();
		List<Record> records = new ArrayList<>();
		for (int key = 1; key < 100; key++) {
			Record record = new Record();
			record.setKey(key);
			record.setValue("val_" + key);
			records.add(record);
		}
		Dataset<Row> recordsDF = sparkSession.createDataFrame(records, Record.class);
		recordsDF.createOrReplaceTempView("records");

		sparkSession.sql("SELECT * FROM records r JOIN src s ON r.key = s.key").show();
		
	}
}
