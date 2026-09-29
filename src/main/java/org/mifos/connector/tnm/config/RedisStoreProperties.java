package org.mifos.connector.tnm.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties(prefix = "tnm-connector.redis")
public class RedisStoreProperties {

    private String keyPrefix = "tnm-connector";
    private Ttl ttl = new Ttl();

    public String getKeyPrefix() {
        return keyPrefix;
    }

    public void setKeyPrefix(String keyPrefix) {
        this.keyPrefix = keyPrefix;
    }

    public Ttl getTtl() {
        return ttl;
    }

    public void setTtl(Ttl ttl) {
        this.ttl = ttl;
    }

    public static class Ttl {

        private long paybillWorkflowSeconds = 172800;

        public long getPaybillWorkflowSeconds() {
            return paybillWorkflowSeconds;
        }

        public void setPaybillWorkflowSeconds(long paybillWorkflowSeconds) {
            this.paybillWorkflowSeconds = paybillWorkflowSeconds;
        }
    }
}
