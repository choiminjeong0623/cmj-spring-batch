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
 * ✅ 본인이 만듦 (2026-11-26 확인): 커밋된 메일을 바로 보내지 않고 발송 대기 테이블에 넣어,
 *    배치가 정해진 수만큼 나눠 발송 — 수량·주기는 예시
 *   → 대량 변경이 있어도 한꺼번에 나가지 않아 메일 발송 부하가 몰리지 않음
 * 📘 개선안 (실제로 한 일인지 미확인):
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
