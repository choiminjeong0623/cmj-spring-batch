package com.cmj.batch.noti;

/** 실제 메일 발송 창구. 예시에서는 로그로만 남기는 구현(LoggingMailSender)을 쓴다 — 실제 메일 서버 연동 방식은 미확인 */
public interface MailSender {
    void send(String to, String title, String content);
}
