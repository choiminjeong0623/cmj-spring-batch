package com.cmj.batch;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.LocalDate;
import java.time.ZoneId;

/**
 * 인사 상태 배치 실행.
 *
 *  - 자동: 매일 현지 자정 (스케줄러)
 *  - 수동: ✅ 실패하면 같은 Java 프로그램을 직접 실행해 다시 돌림 (2026-11-26 확인)
 *          ✅ 수동 실행이면 if문으로 분기해 "마지막 수행자"와 "처리일자"를 잡 파라미터로 넘기도록 로직을 추가
 *
 *  수동 실행 예)
 *    java -jar hr-sync-batch.jar --manual --runBy=choimj --baseDate=2026-11-25
 *
 * ⚠️ 옵션 이름(--manual, --runBy, --baseDate)과 파라미터 이름은 예시
 */
@Component
public class HrSyncScheduler implements ApplicationRunner {

    /** 현지 시간대 — application.yml 의 cmj.batch.zone (예시 값) */
    @org.springframework.beans.factory.annotation.Value("${cmj.batch.zone}")
    private ZoneId localZone;

    private final JobLauncher jobLauncher;
    private final Job hrStatusSyncJob;
    private final ApplicationContext context;

    public HrSyncScheduler(JobLauncher jobLauncher,
                           @org.springframework.beans.factory.annotation.Qualifier("hrStatusSyncJob") Job hrStatusSyncJob, // 알림 발송 Job이 추가되어 이름으로 지정
                           ApplicationContext context) {
        this.jobLauncher = jobLauncher;
        this.hrStatusSyncJob = hrStatusSyncJob;
        this.context = context;
    }

    /** 자동 실행: 수행자는 SYSTEM, 처리일자는 오늘 */
    @Scheduled(cron = "0 0 0 * * *", zone = "${cmj.batch.zone}")
    public void runNightly() throws Exception {
        jobLauncher.run(hrStatusSyncJob, params("SCHEDULED", "SYSTEM", LocalDate.now(localZone).toString()));
    }

    /** 수동 실행: 프로그램을 --manual 로 실행했을 때만 바로 한 번 돌리고 종료 */
    @Override
    public void run(ApplicationArguments args) throws Exception {
        if (!args.containsOption("manual")) {
            return; // 일반 기동이면 스케줄러만 동작
        }
        String runBy = firstOrDefault(args, "runBy", "UNKNOWN");
        String baseDate = firstOrDefault(args, "baseDate", LocalDate.now(localZone).toString());
        jobLauncher.run(hrStatusSyncJob, params("MANUAL", runBy, baseDate));
        System.exit(SpringApplication.exit(context, () -> 0));
    }

    private static JobParameters params(String runType, String runBy, String baseDate) {
        return new JobParametersBuilder()
            .addString("runType", runType)      // SCHEDULED / MANUAL
            .addString("runBy", runBy)          // 마지막 수행자
            .addString("baseDate", baseDate)    // 처리일자
            .addLong("requestedAt", System.currentTimeMillis()) // 같은 날 다시 돌려도 새 실행으로 기록
            .toJobParameters();
    }

    private static String firstOrDefault(ApplicationArguments args, String name, String defaultValue) {
        return args.containsOption(name) && !args.getOptionValues(name).isEmpty()
            ? args.getOptionValues(name).get(0) : defaultValue;
    }
}
