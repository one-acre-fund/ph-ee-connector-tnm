package org.mifos.connector.tnm.flowcomponents;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.time.Duration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mifos.connector.tnm.config.RedisStoreProperties;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class PaybillStateStoreTest {

    private static final String KEY_PREFIX = "test-prefix";
    private static final String TXN_ID = "txn-123";
    private static final String OTHER_TXN_ID = "txn-999";
    private static final long WORKFLOW_TTL_SECONDS = 172800;

    @Mock
    private StringRedisTemplate redisTemplate;

    @Mock
    private ValueOperations<String, String> valueOperations;

    @Mock
    private ObjectProvider<StringRedisTemplate> redisTemplateProvider;

    @BeforeEach
    void setUp() {
        lenient().when(redisTemplate.opsForValue()).thenReturn(valueOperations);
        lenient().when(redisTemplateProvider.getIfAvailable()).thenReturn(redisTemplate);
    }

    @Test
    @DisplayName("Redis put stores value with configured TTL and prefixed key")
    void putWorkflowInstance_shouldStoreWorkflowKeyWithTtl() {
        PaybillStateStore paybillStateStore = redisStore();

        paybillStateStore.putWorkflowInstance(TXN_ID, "workflow-456");

        ArgumentCaptor<Duration> ttlCaptor = ArgumentCaptor.forClass(Duration.class);
        verify(valueOperations).set(org.mockito.ArgumentMatchers.eq(workflowKey(TXN_ID)), org.mockito.ArgumentMatchers.eq("workflow-456"),
                ttlCaptor.capture());
        assertEquals(WORKFLOW_TTL_SECONDS, ttlCaptor.getValue().getSeconds());
    }

    @Test
    @DisplayName("Redis put uses custom TTL from properties")
    void putWorkflowInstance_shouldUseCustomTtl() {
        RedisStoreProperties properties = redisProperties("redis");
        properties.getTtl().setPaybillWorkflowSeconds(60);
        PaybillStateStore paybillStateStore = new PaybillStateStore(redisTemplateProvider, properties);

        paybillStateStore.putWorkflowInstance(TXN_ID, "workflow-456");

        ArgumentCaptor<Duration> ttlCaptor = ArgumentCaptor.forClass(Duration.class);
        verify(valueOperations).set(org.mockito.ArgumentMatchers.eq(workflowKey(TXN_ID)), org.mockito.ArgumentMatchers.eq("workflow-456"),
                ttlCaptor.capture());
        assertEquals(60, ttlCaptor.getValue().getSeconds());
    }

    @Test
    @DisplayName("Redis get returns stored workflow instance")
    void getWorkflowInstance_shouldReturnStoredValue() {
        when(valueOperations.get(workflowKey(TXN_ID))).thenReturn("workflow-456");

        assertEquals("workflow-456", redisStore().getWorkflowInstance(TXN_ID));
    }

    @Test
    @DisplayName("Redis get returns null when key missing")
    void getWorkflowInstance_shouldReturnNullWhenMissing() {
        when(valueOperations.get(workflowKey(TXN_ID))).thenReturn(null);

        assertNull(redisStore().getWorkflowInstance(TXN_ID));
    }

    @Test
    @DisplayName("Redis remove deletes prefixed key")
    void removeWorkflowInstance_shouldDeleteKey() {
        redisStore().removeWorkflowInstance(TXN_ID);

        verify(redisTemplate).delete(workflowKey(TXN_ID));
    }

    @Test
    @DisplayName("Memory store put/get/remove works without touching Redis")
    void memoryStore_shouldPutGetAndRemoveWithoutRedis() {
        PaybillStateStore paybillStateStore = memoryStore();

        paybillStateStore.putWorkflowInstance(TXN_ID, "workflow-456");

        assertEquals("workflow-456", paybillStateStore.getWorkflowInstance(TXN_ID));
        verify(redisTemplate, never()).opsForValue();
        verifyNoInteractions(valueOperations);

        paybillStateStore.removeWorkflowInstance(TXN_ID);
        assertNull(paybillStateStore.getWorkflowInstance(TXN_ID));
        verify(redisTemplate, never()).delete(org.mockito.ArgumentMatchers.anyString());
    }

    @Test
    @DisplayName("Memory store overwrites existing correlation mapping")
    void memoryStore_shouldOverwriteExistingValue() {
        PaybillStateStore paybillStateStore = memoryStore();

        paybillStateStore.putWorkflowInstance(TXN_ID, "workflow-old");
        paybillStateStore.putWorkflowInstance(TXN_ID, "workflow-new");

        assertEquals("workflow-new", paybillStateStore.getWorkflowInstance(TXN_ID));
    }

    @Test
    @DisplayName("Memory store keeps mappings isolated per transaction id")
    void memoryStore_shouldIsolateKeys() {
        PaybillStateStore paybillStateStore = memoryStore();

        paybillStateStore.putWorkflowInstance(TXN_ID, "workflow-a");
        paybillStateStore.putWorkflowInstance(OTHER_TXN_ID, "workflow-b");

        assertEquals("workflow-a", paybillStateStore.getWorkflowInstance(TXN_ID));
        assertEquals("workflow-b", paybillStateStore.getWorkflowInstance(OTHER_TXN_ID));

        paybillStateStore.removeWorkflowInstance(TXN_ID);

        assertNull(paybillStateStore.getWorkflowInstance(TXN_ID));
        assertEquals("workflow-b", paybillStateStore.getWorkflowInstance(OTHER_TXN_ID));
    }

    @Test
    @DisplayName("Memory store returns null for unknown transaction id")
    void memoryStore_shouldReturnNullWhenMissing() {
        assertNull(memoryStore().getWorkflowInstance(TXN_ID));
    }

    @ParameterizedTest
    @ValueSource(strings = { "memory", "MEMORY", "Memory" })
    @DisplayName("Store type memory is case-insensitive")
    void memoryStore_shouldAcceptCaseInsensitiveType(String type) {
        PaybillStateStore paybillStateStore = new PaybillStateStore(redisTemplateProvider, redisProperties(type));

        paybillStateStore.putWorkflowInstance(TXN_ID, "workflow-456");

        assertEquals("workflow-456", paybillStateStore.getWorkflowInstance(TXN_ID));
        verify(redisTemplateProvider, never()).getIfAvailable();
    }

    @Test
    @DisplayName("Redis store fails fast when StringRedisTemplate bean is missing")
    void redisStore_shouldFailWhenRedisTemplateMissing() {
        when(redisTemplateProvider.getIfAvailable()).thenReturn(null);

        IllegalStateException exception = assertThrows(IllegalStateException.class,
                () -> new PaybillStateStore(redisTemplateProvider, redisProperties("redis")));

        assertEquals("tnm-connector.redis.type=redis requires a StringRedisTemplate bean", exception.getMessage());
    }

    @Test
    @DisplayName("Memory store does not require Redis template bean")
    void memoryStore_shouldNotRequireRedisTemplateBean() {
        when(redisTemplateProvider.getIfAvailable()).thenReturn(null);

        PaybillStateStore paybillStateStore = new PaybillStateStore(redisTemplateProvider, redisProperties("memory"));
        paybillStateStore.putWorkflowInstance(TXN_ID, "workflow-456");

        assertEquals("workflow-456", paybillStateStore.getWorkflowInstance(TXN_ID));
    }

    @Test
    @DisplayName("logStoreBackend can be invoked for redis and memory")
    void logStoreBackend_shouldRunForBothBackends() {
        PaybillStateStore redis = redisStore();
        PaybillStateStore memory = memoryStore();

        redis.logStoreBackend();
        memory.logStoreBackend();
    }

    @Test
    @DisplayName("Redis put is invoked once per call")
    void putWorkflowInstance_shouldCallRedisOncePerPut() {
        PaybillStateStore paybillStateStore = redisStore();

        paybillStateStore.putWorkflowInstance(TXN_ID, "workflow-1");
        paybillStateStore.putWorkflowInstance(TXN_ID, "workflow-2");

        verify(valueOperations, times(2)).set(org.mockito.ArgumentMatchers.eq(workflowKey(TXN_ID)),
                org.mockito.ArgumentMatchers.anyString(), org.mockito.ArgumentMatchers.any(Duration.class));
    }

    private PaybillStateStore redisStore() {
        return new PaybillStateStore(redisTemplateProvider, redisProperties("redis"));
    }

    private PaybillStateStore memoryStore() {
        return new PaybillStateStore(redisTemplateProvider, redisProperties("memory"));
    }

    private RedisStoreProperties redisProperties(String type) {
        RedisStoreProperties properties = new RedisStoreProperties();
        properties.setType(type);
        properties.setKeyPrefix(KEY_PREFIX);
        RedisStoreProperties.Ttl ttl = new RedisStoreProperties.Ttl();
        ttl.setPaybillWorkflowSeconds(WORKFLOW_TTL_SECONDS);
        properties.setTtl(ttl);
        return properties;
    }

    private String workflowKey(String txnId) {
        return KEY_PREFIX + ":paybill:workflow:" + txnId;
    }
}
