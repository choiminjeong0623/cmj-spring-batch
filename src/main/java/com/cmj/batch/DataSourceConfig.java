package com.cmj.batch;

import com.zaxxer.hikari.HikariDataSource;
import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;

/**
 * DB 두 개:
 *  - oracle   : 우리 시스템 DB (TB_HR_EMP_STG·TB_HR_EMPLOYEE를 쓰는 곳, Spring Batch 메타 테이블도 여기)
 *  - hrSqlServer : cmj-choiminjeong 내부망의 인사 DB (MS SQL Server, 읽기 전용)
 */
@Configuration
public class DataSourceConfig {

    @Bean @Primary
    @ConfigurationProperties("app.datasource.oracle")
    DataSourceProperties oracleProperties() { return new DataSourceProperties(); }

    @Bean @Primary
    DataSource oracleDataSource() {
        return oracleProperties().initializeDataSourceBuilder().type(HikariDataSource.class).build();
    }

    @Bean
    @ConfigurationProperties("app.datasource.hr")
    DataSourceProperties hrProperties() { return new DataSourceProperties(); }

    @Bean
    DataSource hrSqlServerDataSource() {
        return hrProperties().initializeDataSourceBuilder().type(HikariDataSource.class).build();
    }

    /** 트랜잭션은 쓰는 쪽(Oracle) 기준. 청크마다 커밋 */
    @Bean @Primary
    PlatformTransactionManager transactionManager(DataSource oracleDataSource) {
        return new DataSourceTransactionManager(oracleDataSource);
    }
}
