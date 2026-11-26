package com.cmj.batch.noti;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * 알림 발송 배치 실행 — 1분마다 (⚠️ 주기는 예시)
 * 인사 상태 배치(HrSyncScheduler)와는 별도 스케줄러라 서로 영향 없음.
 */
@Component
public class NotiOutboxScheduler {

    private final JobLauncher jobLauncher;
    private final Job notiOutboxSendJob;

    public NotiOutboxScheduler(JobLauncher jobLauncher, @Qualifier("notiOutboxSendJob") Job notiOutboxSendJob) {
        this.jobLauncher = jobLauncher;
        this.notiOutboxSendJob = notiOutboxSendJob;
    }

    @Scheduled(fixedDelayString = "${cmj.noti.interval-ms:60000}")
    public void run() throws Exception {
        jobLauncher.run(notiOutboxSendJob, new JobParametersBuilder()
            .addLong("runAt", System.currentTimeMillis())   // 매번 새 실행으로 인식되도록
            .toJobParameters());
    }
}
