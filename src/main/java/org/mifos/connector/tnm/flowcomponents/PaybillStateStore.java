package org.mifos.connector.tnm.flowcomponents;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.PostConstruct;
import org.mifos.connector.tnm.config.RedisStoreProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

@Component
public class PaybillStateStore {

    private static final Logger log = LoggerFactory.getLogger(PaybillStateStore.class);
    private static final String WORKFLOW_KEY_PREFIX = "paybill:workflow:";

    private final boolean useMemoryStore;
    private final StringRedisTemplate redisTemplate;
    private final Map<String, String> workflowInstanceStore = new ConcurrentHashMap<>();
    private final String keyPrefix;
    private final long workflowTtlSeconds;

    public PaybillStateStore(ObjectProvider<StringRedisTemplate> redisTemplateProvider, RedisStoreProperties redisStoreProperties) {
        this.useMemoryStore = redisStoreProperties.isMemoryStore();
        this.keyPrefix = redisStoreProperties.getKeyPrefix();
        this.workflowTtlSeconds = redisStoreProperties.getTtl().getPaybillWorkflowSeconds();
        if (useMemoryStore) {
            this.redisTemplate = null;
        } else {
            StringRedisTemplate template = redisTemplateProvider.getIfAvailable();
            if (template == null) {
                throw new IllegalStateException("tnm-connector.redis.type=redis requires a StringRedisTemplate bean");
            }
            this.redisTemplate = template;
        }
    }

    @PostConstruct
    void logStoreBackend() {
        log.info("Paybill workflow correlation store backend: {}", useMemoryStore ? "memory" : "redis");
    }

    public void putWorkflowInstance(String txnId, String workflowInstanceKey) {
        if (useMemoryStore) {
            workflowInstanceStore.put(workflowKey(txnId), workflowInstanceKey);
            return;
        }
        redisTemplate.opsForValue().set(workflowKey(txnId), workflowInstanceKey, Duration.ofSeconds(workflowTtlSeconds));
    }

    public String getWorkflowInstance(String txnId) {
        if (useMemoryStore) {
            return workflowInstanceStore.get(workflowKey(txnId));
        }
        return redisTemplate.opsForValue().get(workflowKey(txnId));
    }

    public void removeWorkflowInstance(String txnId) {
        if (useMemoryStore) {
            workflowInstanceStore.remove(workflowKey(txnId));
            return;
        }
        redisTemplate.delete(workflowKey(txnId));
    }

    private String workflowKey(String txnId) {
        return keyPrefix + ":" + WORKFLOW_KEY_PREFIX + txnId;
    }
}
