package cn.com.bsfit;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.security.SecurityAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;

@EnableBatchProcessing
@SpringBootApplication(exclude = { SecurityAutoConfiguration.class })
public class SparkApp implements CommandLineRunner {

	@Autowired
	private JobLauncher jobLauncher;

	@Autowired
	private Job tweetTopHashtags;

	@Override
	public void run(final String... args) throws Exception {
		jobLauncher.run(tweetTopHashtags, new JobParametersBuilder().toJobParameters());
	}

	public static void main(final String[] args) {
		new SpringApplicationBuilder().sources(SparkApp.class).showBanner(false).run(args);
	}

}
