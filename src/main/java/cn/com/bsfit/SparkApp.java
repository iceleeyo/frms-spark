package cn.com.bsfit;

import org.springframework.boot.Banner;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;

@SpringBootApplication(exclude = { DataSourceAutoConfiguration.class })
public class SparkApp {

	public static void main(final String[] args) {
		new SpringApplicationBuilder().sources(SparkApp.class).bannerMode(Banner.Mode.OFF).run(args);
	}

}
