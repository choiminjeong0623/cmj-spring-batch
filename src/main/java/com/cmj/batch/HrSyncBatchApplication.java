package com.cmj.batch;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class HrSyncBatchApplication {
    public static void main(String[] args) {
        SpringApplication.run(HrSyncBatchApplication.class, args);
    }
}
