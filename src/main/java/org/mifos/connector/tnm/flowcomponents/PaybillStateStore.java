package org.mifos.connector.tnm.flowcomponents;

import java.time.Duration;
import org.mifos.connector.tnm.config.RedisStoreProperties;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

@Component
public class PaybillStateStore {

    private static final String WORKFLOW_KEY_PREFIX = "paybill:workflow:";

    private final StringRedisTemplate redisTemplate;
    private final String keyPrefix;
    private final long workflowTtlSeconds;

    public PaybillStateStore(StringRedisTemplate redisTemplate, RedisStoreProperties redisStoreProperties) {
        this.redisTemplate = redisTemplate;
        this.keyPrefix = redisStoreProperties.getKeyPrefix();
        this.workflowTtlSeconds = redisStoreProperties.getTtl().getPaybillWorkflowSeconds();
    }

    public void putWorkflowInstance(String txnId, String workflowInstanceKey) {
        redisTemplate.opsForValue().set(workflowKey(txnId), workflowInstanceKey, Duration.ofSeconds(workflowTtlSeconds));
    }

    public String getWorkflowInstance(String txnId) {
        return redisTemplate.opsForValue().get(workflowKey(txnId));
    }

    public void removeWorkflowInstance(String txnId) {
        redisTemplate.delete(workflowKey(txnId));
    }

    private String workflowKey(String txnId) {
        return keyPrefix + ":" + WORKFLOW_KEY_PREFIX + txnId;
    }
}
