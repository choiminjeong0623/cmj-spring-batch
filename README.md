# cmj-spring-batch

> ⚠️ **참고용 예시 코드입니다.** 실제 IDRAS 프로젝트 코드가 아니며, 면접 대비를 위해 기억하는 구조와 대화로 확정한 사항을 바탕으로 다시 짠 것입니다.
> 의존성 다운로드가 막힌 환경에서 작성해 **컴파일·실행은 검증하지 않았습니다.**

## 역할
Spring Batch. 매일 0시 인사 DB(MS SQL) → 스테이징 테이블 → 한 트랜잭션에서 DELETE + INSERT…SELECT로 TB_HR_EMPLOYEE 전체 교체. 수동 실행 시 --manual --runBy= --baseDate= 로 실행자·기준일 기록.

## 빌드
```bash
./gradlew build
```
(공통 보안 라이브러리에 의존하지 않음)

## 관련 저장소
| 저장소 | 역할 |
|---|---|
| cmj-common-security | 공통 보안 라이브러리 (서버) — Redis 세션·요청마다 세션·권한 검사 |
| cmj-gateway | 게이트웨이 — JWT 서명·만료 확인 |
| cmj-auth-server | 토큰 발급 (OAuth2) |
| cmj-security-server | 로그인 규칙 |
| cmj-portal-app | 포털·관리자 API |
| cmj-spring-batch | 인사 동기화 배치 |
| cmj-redis | Redis에 저장할 값(라벨·공통 코드 등) 정의 + 기동 시 DB→Redis 적재 |
| cmj-frontend-common | 공통 화면 코드 |
| cmj-frontend-service | 업무 화면 코드 |

⚠️ 공통 라이브러리의 실제 배포 방식(사내 패키지 저장소 등)은 확인되지 않아, 예시는 mavenLocal 을 씁니다.
