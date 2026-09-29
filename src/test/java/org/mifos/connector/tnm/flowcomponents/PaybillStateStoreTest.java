package org.mifos.connector.tnm.flowcomponents;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Duration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mifos.connector.tnm.config.RedisStoreProperties;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class PaybillStateStoreTest {

    private static final String KEY_PREFIX = "test-prefix";
    private static final String TXN_ID = "txn-123";
    private static final long WORKFLOW_TTL_SECONDS = 172800;

    @Mock
    private StringRedisTemplate redisTemplate;

    @Mock
    private ValueOperations<String, String> valueOperations;

    private PaybillStateStore paybillStateStore;

    @BeforeEach
    void setUp() {
        RedisStoreProperties properties = new RedisStoreProperties();
        properties.setKeyPrefix(KEY_PREFIX);
        RedisStoreProperties.Ttl ttl = new RedisStoreProperties.Ttl();
        ttl.setPaybillWorkflowSeconds(WORKFLOW_TTL_SECONDS);
        properties.setTtl(ttl);

        lenient().when(redisTemplate.opsForValue()).thenReturn(valueOperations);
        paybillStateStore = new PaybillStateStore(redisTemplate, properties);
    }

    @Test
    void putWorkflowInstance_shouldStoreWorkflowKeyWithTtl() {
        paybillStateStore.putWorkflowInstance(TXN_ID, "workflow-456");

        ArgumentCaptor<Duration> ttlCaptor = ArgumentCaptor.forClass(Duration.class);
        verify(valueOperations).set(org.mockito.ArgumentMatchers.eq(workflowKey(TXN_ID)), org.mockito.ArgumentMatchers.eq("workflow-456"),
                ttlCaptor.capture());
        assertEquals(WORKFLOW_TTL_SECONDS, ttlCaptor.getValue().getSeconds());
    }

    @Test
    void getWorkflowInstance_shouldReturnStoredValue() {
        when(valueOperations.get(workflowKey(TXN_ID))).thenReturn("workflow-456");

        assertEquals("workflow-456", paybillStateStore.getWorkflowInstance(TXN_ID));
    }

    @Test
    void getWorkflowInstance_shouldReturnNullWhenMissing() {
        when(valueOperations.get(workflowKey(TXN_ID))).thenReturn(null);

        assertNull(paybillStateStore.getWorkflowInstance(TXN_ID));
    }

    @Test
    void removeWorkflowInstance_shouldDeleteKey() {
        paybillStateStore.removeWorkflowInstance(TXN_ID);

        verify(redisTemplate).delete(workflowKey(TXN_ID));
    }

    private String workflowKey(String txnId) {
        return KEY_PREFIX + ":paybill:workflow:" + txnId;
    }
}
