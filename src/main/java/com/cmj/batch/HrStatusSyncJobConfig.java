package com.cmj.batch;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.database.builder.JdbcCursorItemReaderBuilder;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SimplePropertySqlParameterSource;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;
import java.sql.Date;
import java.sql.Timestamp;

/**
 * 인사 DB(MS SQL)의 직원 정보를 매일 우리 Oracle의 TB_HR_EMPLOYEE로 "전체 복사"한다.
 * 시큐리티 서버는 로그인할 때 TB_HR_EMPLOYEE의 인사 상태만 본다 (인사 DB를 로그인마다 직접 조회하지 않음).
 *
 * ✅ 매일 전체 복사 (대상 1만 명 미만이라 부담이 크지 않았음)
 * ✅ 실패 확인: 로그·ELK + 매일 아침 마지막 실행 일자 쿼리 / ✅ 실패 시 수동 실행(수행자·처리일자 파라미터)
 * ⚠️ 인사 DB의 실제 테이블·컬럼은 기억 안 남 → 아래는 "일반적인 공무원 인사 정보"로 만든 예시
 *
 * 왜 3단계인가
 *   MS SQL과 Oracle은 서로 다른 DB라서, 한 SQL 문으로 "MS SQL에서 SELECT → Oracle에 INSERT"를 할 수 없다.
 *   (DB 링크/게이트웨이를 따로 구성하면 가능하지만 인프라 작업이 필요)
 *   그래서 ① 우리 Oracle에 "적재용 임시 테이블(스테이징)"을 비우고
 *        ② Spring Batch로 MS SQL에서 읽어 스테이징에 그대로 넣은 뒤
 *        ③ Oracle 안에서 INSERT ... SELECT 한 번으로 본 테이블을 통째로 바꾼다.
 *
 *   [MS SQL] dbo.HR_EMPLOYEE ──① 비우기 ②Spring Batch 복사──▶ [Oracle] TB_HR_EMP_STG ──③ INSERT…SELECT──▶ TB_HR_EMPLOYEE
 */
@Configuration
public class HrStatusSyncJobConfig {

    /** 인사 DB 한 행 (예시 컬럼) */
    public record HrEmployee(
        String empNo, String empNm, String deptCd, String deptNm, String positionCd, String gradeCd,
        String email, String mobileNo, Date hireDt, Date retireDt,
        String empStatusCd, Date leaveStartDt, Date leaveEndDt, Timestamp hrUpdatedAt) {}

    @Bean
    Job hrStatusSyncJob(JobRepository jobRepository, Step clearStagingStep, Step extractToStagingStep,
                        Step replaceEmployeeStep) {
        return new JobBuilder("hrStatusSyncJob", jobRepository)
            .incrementer(new RunIdIncrementer())
            .start(clearStagingStep)       // ① 스테이징 비우기
            .next(extractToStagingStep)    // ② MS SQL → 스테이징
            .next(replaceEmployeeStep)     // ③ 스테이징 → 본 테이블 (INSERT ... SELECT)
            .build();
    }

    // ───────────────────────── ① 스테이징 비우기 ─────────────────────────
    @Bean
    Step clearStagingStep(JobRepository jobRepository, PlatformTransactionManager tx, DataSource oracleDataSource) {
        Tasklet clear = (contribution, chunkContext) -> {
            // 스테이징은 로그인에서 보지 않으므로 TRUNCATE(빠름)해도 안전
            new JdbcTemplate(oracleDataSource).execute("TRUNCATE TABLE TB_HR_EMP_STG");
            return RepeatStatus.FINISHED;
        };
        return new StepBuilder("clearStagingStep", jobRepository).tasklet(clear, tx).build();
    }

    // ───────────────────────── ② MS SQL → 스테이징 ─────────────────────────
    @Bean
    Step extractToStagingStep(JobRepository jobRepository, PlatformTransactionManager tx,
                              JdbcCursorItemReader<HrEmployee> hrReader, JdbcBatchItemWriter<HrEmployee> stagingWriter) {
        return new StepBuilder("extractToStagingStep", jobRepository)
            .<HrEmployee, HrEmployee>chunk(500, tx)   // 500건씩 읽고 써서 커밋
            .reader(hrReader)
            .writer(stagingWriter)
            .build();
    }

    /** MS SQL 쪽은 "읽기만" 한다. 직원 + 부서 이름 (⚠️ 테이블·컬럼은 예시) */
    @Bean
    JdbcCursorItemReader<HrEmployee> hrReader(@Qualifier("hrSqlServerDataSource") DataSource hr) {
        return new JdbcCursorItemReaderBuilder<HrEmployee>()
            .name("hrReader")
            .dataSource(hr)
            .sql("""
                SELECT E.EMP_NO, E.EMP_NM, E.DEPT_CD, D.DEPT_NM, E.POSITION_CD, E.GRADE_CD,
                       E.EMAIL, E.MOBILE_NO, E.HIRE_DT, E.RETIRE_DT,
                       E.EMP_STATUS_CD, E.LEAVE_START_DT, E.LEAVE_END_DT, E.UPDATED_AT
                  FROM dbo.HR_EMPLOYEE E
                  LEFT JOIN dbo.HR_DEPARTMENT D ON D.DEPT_CD = E.DEPT_CD""")
            .rowMapper((rs, i) -> new HrEmployee(
                rs.getString("EMP_NO"), rs.getString("EMP_NM"), rs.getString("DEPT_CD"), rs.getString("DEPT_NM"),
                rs.getString("POSITION_CD"), rs.getString("GRADE_CD"), rs.getString("EMAIL"), rs.getString("MOBILE_NO"),
                rs.getDate("HIRE_DT"), rs.getDate("RETIRE_DT"), rs.getString("EMP_STATUS_CD"),
                rs.getDate("LEAVE_START_DT"), rs.getDate("LEAVE_END_DT"), rs.getTimestamp("UPDATED_AT")))
            .fetchSize(1000)
            .build();
    }

    /** 읽은 값을 가공 없이 스테이징에 그대로 넣는다 (가공은 ③에서 SQL로) */
    @Bean
    JdbcBatchItemWriter<HrEmployee> stagingWriter(DataSource oracleDataSource) {
        return new JdbcBatchItemWriterBuilder<HrEmployee>()
            .dataSource(oracleDataSource)
            .sql("""
                INSERT INTO TB_HR_EMP_STG
                       (EMP_NO, EMP_NM, DEPT_CD, DEPT_NM, POSITION_CD, GRADE_CD, EMAIL, MOBILE_NO,
                        HIRE_DT, RETIRE_DT, EMP_STATUS_CD, LEAVE_START_DT, LEAVE_END_DT, HR_UPDATED_AT, LOADED_AT)
                VALUES (:empNo, :empNm, :deptCd, :deptNm, :positionCd, :gradeCd, :email, :mobileNo,
                        :hireDt, :retireDt, :empStatusCd, :leaveStartDt, :leaveEndDt, :hrUpdatedAt, SYSTIMESTAMP)""")
            // 레코드의 empNo() 등 접근자 이름으로 :empNo 를 채움 (beanMapped()는 getEmpNo() 같은 getter만 읽어서 레코드에 안 맞음)
            .itemSqlParameterSourceProvider(SimplePropertySqlParameterSource::new)
            .build();
    }

    // ───────────────────────── ③ 스테이징 → 본 테이블 (INSERT ... SELECT) ─────────────────────────
    @Bean
    Step replaceEmployeeStep(JobRepository jobRepository, PlatformTransactionManager tx, Tasklet replaceEmployeeTasklet) {
        // tasklet 하나 = 트랜잭션 하나 → DELETE와 INSERT…SELECT가 함께 커밋되거나 함께 취소됨
        return new StepBuilder("replaceEmployeeStep", jobRepository).tasklet(replaceEmployeeTasklet, tx).build();
    }

    @Bean
    @StepScope
    Tasklet replaceEmployeeTasklet(DataSource oracleDataSource,
                                   @Value("#{jobParameters['runBy']}") String runBy,
                                   @Value("#{jobParameters['runType']}") String runType) {
        return (contribution, chunkContext) -> {
            NamedParameterJdbcTemplate jdbc = new NamedParameterJdbcTemplate(oracleDataSource);

            // 안전장치: 인사 DB에서 한 건도 못 읽었으면(연결 문제 등) 본 테이블을 지우지 않고 실패 처리
            Integer staged = jdbc.getJdbcTemplate().queryForObject("SELECT COUNT(*) FROM TB_HR_EMP_STG", Integer.class);
            if (staged == null || staged == 0) {
                throw new IllegalStateException("스테이징이 비어 있어 TB_HR_EMPLOYEE를 바꾸지 않습니다.");
            }

            // TRUNCATE는 바로 커밋되어 되돌릴 수 없고, 그 사이 로그인이 빈 테이블을 볼 수 있음 → DELETE 사용
            jdbc.getJdbcTemplate().update("DELETE FROM TB_HR_EMPLOYEE");

            int inserted = jdbc.update("""
                INSERT INTO TB_HR_EMPLOYEE
                       (EMPLOYEE_NO, EMP_NM, DEPT_CD, DEPT_NM, POSITION_CD, GRADE_CD, EMAIL, MOBILE_NO,
                        HIRE_DT, RETIRE_DT, EMP_STATUS, LEAVE_START_DT, LEAVE_END_DT,
                        HR_UPDATED_AT, SYNCED_AT, SYNCED_BY, SYNC_TYPE)
                SELECT TRIM(S.EMP_NO),
                       TRIM(S.EMP_NM),
                       S.DEPT_CD,
                       S.DEPT_NM,
                       S.POSITION_CD,
                       S.GRADE_CD,
                       LOWER(TRIM(S.EMAIL)),
                       S.MOBILE_NO,
                       S.HIRE_DT,
                       S.RETIRE_DT,
                       -- 인사 DB 상태 코드를 우리 시스템 상태로 바꿈 (로그인 차단 판단에 쓰는 값)
                       CASE
                           WHEN S.RETIRE_DT IS NOT NULL AND S.RETIRE_DT <= TRUNC(SYSDATE) THEN 'RETIRED'
                           WHEN S.EMP_STATUS_CD = 'L' THEN 'ON_LEAVE'     -- 휴직
                           WHEN S.EMP_STATUS_CD = 'S' THEN 'SUSPENDED'    -- 정직
                           WHEN S.EMP_STATUS_CD = 'R' THEN 'RETIRED'      -- 퇴직
                           WHEN S.EMP_STATUS_CD = 'A' THEN 'ACTIVE'       -- 재직
                           ELSE 'UNKNOWN'
                       END,
                       S.LEAVE_START_DT,
                       S.LEAVE_END_DT,
                       S.HR_UPDATED_AT,
                       SYSTIMESTAMP,
                       :runBy,
                       :runType
                  FROM TB_HR_EMP_STG S
                 WHERE S.EMP_NO IS NOT NULL""",
                new MapSqlParameterSource().addValue("runBy", runBy).addValue("runType", runType));

            contribution.incrementWriteCount(inserted);  // 실행 기록(BATCH_STEP_EXECUTION)에 건수가 남음
            return RepeatStatus.FINISHED;
        };
    }
}
