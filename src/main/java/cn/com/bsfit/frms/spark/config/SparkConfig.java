package cn.com.bsfit.frms.spark.config;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import cn.com.bsfit.frms.spark.utils.Constants;

/**
 * spark config
 * 
 * @author hjp
 * 
 * @since 1.0.0
 *
 */
@Configuration
public class SparkConfig {

//	@Bean
//	public JavaStreamingContext javaStreamingContext() {
//		final SparkConf sparkConf = new SparkConf();
//		sparkConf.setAppName(Constants.APP_NAME).setMaster("local[*]");
//		return new JavaStreamingContext(sparkConf, new Duration(1000));
//	}

	@Bean(destroyMethod = "stop")
	public SparkSession sparkSession() {
		System.setProperty("hadoop.home.dir", "D:/Program Files/Hadoop");
		return SparkSession.builder().appName(Constants.APP_NAME).master("local[*]")
				.config("spark.sql.warehouse.dir", "file:///").getOrCreate();
	}
}
