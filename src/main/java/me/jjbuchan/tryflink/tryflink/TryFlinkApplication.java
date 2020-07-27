package me.jjbuchan.tryflink.tryflink;

import me.jjbuchan.tryflink.tryflink.service.DataProcessor;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class TryFlinkApplication {

  public static void main(String[] args) throws Exception {
    SpringApplication.run(TryFlinkApplication.class, args);

    DataProcessor.capitalize();
    DataProcessor.join();
  }

}
