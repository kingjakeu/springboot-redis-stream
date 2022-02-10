# springboot-redis-stream

## Overview
Spring Boot에서의 Redis Stream 활용 예제 구현

관련 내용 정리 블로그 post 
- [https://kingjakeu.github.io/springboot/2022/02/10/spring-boot-redis-stream/](https://kingjakeu.github.io/springboot/2022/02/10/spring-boot-redis-stream/)

## 구현 구성

- Consumer Bean을 통해, Consumer Group을 설정하고, Consumer Group을 통해 Redis Stream의 메시지를 Event-Driven 형태로 Subscribe
- PendingMessageScheduler를 통해, Consumer Bean에서 처리 중 에러가 발생한 `pending`상태의 메시지 처리
