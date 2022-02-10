package com.example.redisstream.scheduler;

import com.example.redisstream.util.RedisOperator;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.PendingMessage;
import org.springframework.data.redis.connection.stream.PendingMessages;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Slf4j
@EnableScheduling
@Component
@RequiredArgsConstructor
public class PendingMessageScheduler implements InitializingBean {

    private String streamKey;
    private String consumerGroupName;
    private String consumerName;
    private final RedisOperator redisOperator;

    @Scheduled(fixedRate = 10000) // 10초 마다 작동
    public void processPendingMessage(){
        // Pending message 조회
        PendingMessages pendingMessages = this.redisOperator
                .findStreamPendingMessages(streamKey, consumerGroupName, consumerName);

        for(PendingMessage pendingMessage : pendingMessages){
            // claim을 통해 consumer 변경
            this.redisOperator.claimStream(pendingMessage, consumerName);
            try{
                // Stream message 조회
                MapRecord<String, Object, Object> messageToProcess = this.redisOperator
                        .findStreamMessageById(this.streamKey, pendingMessage.getIdAsString());
                if(messageToProcess == null){
                    log.info("존재하지 않는 메시지");
                }else{
                    // 해당 메시지 에러 발생 횟수 확인
                    int errorCount = (int) this.redisOperator
                            .getRedisValue("errorCount", pendingMessage.getIdAsString());

                    // 에러 5회이상 발생
                    if(errorCount >= 5){
                        log.info("재 처리 최대 시도 횟수 초과");
                    }
                    // 두개 이상의 consumer에게 delivered 된 메시지
                    else if(pendingMessage.getTotalDeliveryCount() >= 2){
                        log.info("최대 delivery 횟수 초과");
                    }else{
                        // 처리할 로직 구현 ex) service.someServiceMethod();
                    }
                    // ack stream
                    this.redisOperator.ackStream(consumerGroupName, messageToProcess);
                }
            }catch (Exception e){
                // 해당 메시지 에러 발생 횟수 + 1
                this.redisOperator.increaseRedisValue("errorCount", pendingMessage.getIdAsString());
            }
        }
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        this.streamKey = "mystream";
        this.consumerGroupName = "consumerGroupName";
        this.consumerName = "consumerName";
    }
}
