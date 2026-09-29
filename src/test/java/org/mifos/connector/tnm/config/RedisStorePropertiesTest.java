package org.mifos.connector.tnm.config;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class RedisStorePropertiesTest {

    @Test
    void defaults_shouldProvideExpectedValues() {
        RedisStoreProperties properties = new RedisStoreProperties();

        assertEquals("tnm-connector", properties.getKeyPrefix());
        assertEquals(172800, properties.getTtl().getPaybillWorkflowSeconds());
    }

    @Test
    void setters_shouldUpdateConfiguration() {
        RedisStoreProperties properties = new RedisStoreProperties();
        RedisStoreProperties.Ttl ttl = new RedisStoreProperties.Ttl();
        ttl.setPaybillWorkflowSeconds(300);

        properties.setKeyPrefix("custom-prefix");
        properties.setTtl(ttl);

        assertEquals("custom-prefix", properties.getKeyPrefix());
        assertEquals(300, properties.getTtl().getPaybillWorkflowSeconds());
    }
}
