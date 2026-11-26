package com.cmj.batch.noti;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/** ⚠️ 예시 구현: 메일을 보내는 대신 로그에 남김 (로컬·테스트용) */
@Component
public class LoggingMailSender implements MailSender {

    private static final Logger log = LoggerFactory.getLogger(LoggingMailSender.class);

    @Override
    public void send(String to, String title, String content) {
        if (to == null || to.isBlank()) {
            throw new IllegalStateException("메일 주소 없음");
        }
        log.info("[mail] to={} title={}", to, title);
    }
}
