package org.opennms.core.daemon.common;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.annotation.EnableTransactionManagement;

class DaemonDataSourceConfigurationTest {

    @Test
    void hasRequiredAnnotations() {
        assertThat(DaemonDataSourceConfiguration.class.isAnnotationPresent(Configuration.class)).isTrue();
        assertThat(DaemonDataSourceConfiguration.class.isAnnotationPresent(EnableTransactionManagement.class)).isTrue();
    }

    @Test
    void hasConditionalOnPropertyGuard() {
        ConditionalOnProperty annotation = DaemonDataSourceConfiguration.class
            .getAnnotation(ConditionalOnProperty.class);
        assertThat(annotation).isNotNull();
        assertThat(annotation.name()).containsExactly("spring.datasource.url");
    }

    @Test
    void entityScanIsNotOnSharedClass() {
        // @EntityScan is intentionally NOT on DaemonDataSourceConfiguration.
        // Each Spring Boot application places @EntityScan on its own configuration
        // class to scan the correct entity package for that daemon.
        assertThat(DaemonDataSourceConfiguration.class.getAnnotations())
                .noneMatch(a -> a.annotationType().getSimpleName().equals("EntityScan"));
    }
}
