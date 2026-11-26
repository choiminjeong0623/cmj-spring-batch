package com.cmj.batch.noti;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;
import java.util.List;
import java.util.Map;

/**
 * 알림 발송 배치: 발송 대기 테이블(TB_NOTI_OUTBOX)의 READY 건을 조금씩 꺼내 보낸다.
 *
 * 🟩 본인이 수정에 참여해 관리자 화면·발송 대기 테이블 쪽 로직을 맡았다고 기억함
 *    — 본인 역할 범위는 기억 정리 중, 테이블 구조·발송 주기·건수 등 세부는 예시
 * 📘 개선안 (실제로 한 일인지 미확인):
 *   - 한 번에 최대 N건만 → 대량 변경이 있어도 한꺼번에 나가지 않고, 이상을 보면 남은 건을 멈출 시간이 생김
 *   - 정책 라벨 MAIL_SEND_YN = N 이면 아무것도 보내지 않고 끝 (배포 없이 발송 중지 스위치)
 *   - 실패하면 RETRY_CNT 를 올리고, 3번을 넘으면 FAILED 로 남겨 운영자가 확인
 */
@Configuration
public class NotiOutboxSendJobConfig {

    private static final Logger log = LoggerFactory.getLogger(NotiOutboxSendJobConfig.class);
    private static final int MAX_RETRY = 3;

    @Bean
    Job notiOutboxSendJob(JobRepository jobRepository, Step notiOutboxSendStep) {
        return new JobBuilder("notiOutboxSendJob", jobRepository).start(notiOutboxSendStep).build();
    }

    @Bean
    Step notiOutboxSendStep(JobRepository jobRepository, PlatformTransactionManager tx, Tasklet notiOutboxSendTasklet) {
        return new StepBuilder("notiOutboxSendStep", jobRepository).tasklet(notiOutboxSendTasklet, tx).build();
    }

    @Bean
    Tasklet notiOutboxSendTasklet(DataSource oracleDataSource, MailSender mailSender,
                                  @Value("${cmj.noti.batch-size:100}") int batchSize) {
        NamedParameterJdbcTemplate jdbc = new NamedParameterJdbcTemplate(oracleDataSource);
        return (contribution, chunkContext) -> {
            // 발송 중지 스위치 — 배치는 Redis를 쓰지 않으므로 원본(TB_LABEL)을 직접 읽음
            String sendYn = jdbc.queryForList("""
                SELECT LABEL_TEXT FROM TB_LABEL
                 WHERE LABEL_GRP = 'POLICY' AND LANG_CD = 'SYS' AND LABEL_KEY = 'MAIL_SEND_YN' AND USE_YN = 'Y'""",
                Map.of(), String.class).stream().findFirst().orElse("Y");
            if (!"Y".equalsIgnoreCase(sendYn.trim())) {
                log.warn("[noti] MAIL_SEND_YN={} → 발송하지 않음", sendYn);
                return RepeatStatus.FINISHED;
            }

            // 오래된 순으로 최대 N건 (⚠️ 서버가 여러 대면 같은 건을 집지 않도록 잠금·담당 표시가 필요 — 예시는 1대 기준)
            List<Map<String, Object>> rows = jdbc.queryForList("""
                SELECT * FROM (
                    SELECT O.CHANGE_ID, O.USER_ID, O.TITLE, O.CONTENT, O.RETRY_CNT, U.EMAIL
                      FROM TB_NOTI_OUTBOX O
                      LEFT JOIN TB_USER U ON U.USER_ID = O.USER_ID
                     WHERE O.STATUS = 'READY'
                     ORDER BY O.REG_DT
                ) WHERE ROWNUM <= :n""", Map.of("n", batchSize));

            int sent = 0, failed = 0;
            for (Map<String, Object> r : rows) {
                Map<String, Object> key = Map.of("c", r.get("CHANGE_ID"), "u", r.get("USER_ID"));
                try {
                    mailSender.send((String) r.get("EMAIL"), (String) r.get("TITLE"), (String) r.get("CONTENT"));
                    jdbc.update("""
                        UPDATE TB_NOTI_OUTBOX SET STATUS = 'SENT', SENT_DT = SYSTIMESTAMP
                         WHERE CHANGE_ID = :c AND USER_ID = :u AND STATUS = 'READY'""", key);
                    sent++;
                } catch (Exception e) {
                    int retry = ((Number) r.get("RETRY_CNT")).intValue() + 1;
                    jdbc.update("""
                        UPDATE TB_NOTI_OUTBOX
                           SET RETRY_CNT = :retry,
                               STATUS = CASE WHEN :retry > :max THEN 'FAILED' ELSE 'READY' END,
                               LAST_ERROR = SUBSTR(:err, 1, 500)
                         WHERE CHANGE_ID = :c AND USER_ID = :u""",
                        Map.of("retry", retry, "max", MAX_RETRY, "err", String.valueOf(e.getMessage()),
                               "c", r.get("CHANGE_ID"), "u", r.get("USER_ID")));
                    failed++;
                }
            }
            contribution.incrementWriteCount(sent);
            log.info("[noti] picked={} sent={} failed={}", rows.size(), sent, failed);
            return RepeatStatus.FINISHED;
        };
    }
}
