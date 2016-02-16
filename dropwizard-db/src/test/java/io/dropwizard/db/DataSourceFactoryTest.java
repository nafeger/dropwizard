package io.dropwizard.db;

import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Optional;
import io.dropwizard.configuration.ConfigurationFactory;
import io.dropwizard.configuration.ResourceConfigurationSourceProvider;
import io.dropwizard.jackson.Jackson;
import io.dropwizard.util.Duration;
import io.dropwizard.validation.BaseValidator;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.fail;

public class DataSourceFactoryTest {
    private final MetricRegistry metricRegistry = new MetricRegistry();

    private DataSourceFactory factory;
    private ManagedDataSource dataSource;

    @Before
    public void setUp() {
        factory = new DataSourceFactory();
        factory.setUrl("jdbc:h2:mem:DbTest-" + System.currentTimeMillis() + ";user=sa");
        factory.setDriverClass("org.h2.Driver");
        factory.setValidationQuery("SELECT 1");
    }

    @After
    public void tearDown() throws Exception {
        if (null != dataSource) {
            dataSource.stop();
        }
    }

    private ManagedDataSource dataSource() throws Exception {
        dataSource = factory.build(metricRegistry, "test");
        dataSource.start();
        return dataSource;
    }

    @Test
    public void buildsAConnectionPoolToTheDatabase() throws Exception {
        try (Connection connection = dataSource().getConnection()) {
            try (PreparedStatement statement = connection.prepareStatement("select 1")) {
                try (ResultSet set = statement.executeQuery()) {
                    while (set.next()) {
                        assertThat(set.getInt(1)).isEqualTo(1);
                    }
                }
            }
        }
    }

    @Test
    public void testNoValidationQueryTimeout() throws Exception {
        try (Connection connection = dataSource().getConnection()) {
            try (PreparedStatement statement = connection.prepareStatement("select 1")) {
                assertThat(statement.getQueryTimeout()).isEqualTo(0);
            }
        }
    }

    @Test
    public void testValidationQueryTimeoutIsSet() throws Exception {
        factory.setValidationQueryTimeout(Duration.seconds(3));
        // want this to be larger due to hikari internals
        factory.setMaxWaitForConnection(Duration.seconds(5));
        dataSource();
        // Something appears to be delayed in hikari on a mbp it seems like 450 ms sleep fails 50% of the time
        // a one second sleep does not appear to fail at all for me. sigh.
        Thread.sleep(1000);
        try (Connection connection = dataSource.getConnection()) {
            try (PreparedStatement statement = connection.prepareStatement("select 1")) {
                assertThat(statement.getQueryTimeout()).isEqualTo(3);
            }
        }
    }

    @Test
    public void invalidJDBCDriverClassThrowsSQLException() throws SQLException {
        final DataSourceFactory factory = new DataSourceFactory();
        factory.setDriverClass("org.example.no.driver.here");

        try {
            factory.build(metricRegistry, "test").getConnection();
            fail("Expected exception");
        } catch (RuntimeException e) {
            assertThat(e.getMessage()).contains("Could not load class of driverClassName");
        }
    }

    @Test
    public void createDefaultFactory() throws Exception {
        final DataSourceFactory factory = new ConfigurationFactory<>(DataSourceFactory.class,
            BaseValidator.newValidator(), Jackson.newObjectMapper(), "dw")
            .build(new ResourceConfigurationSourceProvider(), "yaml/minimal_db_pool.yml");

        assertThat(factory.getDriverClass()).isEqualTo("org.postgresql.Driver");
        assertThat(factory.getUser()).isEqualTo("pg-user");
        assertThat(factory.getPassword()).isEqualTo("iAMs00perSecrEET");
        assertThat(factory.getUrl()).isEqualTo("jdbc:postgresql://db.example.com/db-prod");
        assertThat(factory.getValidationQuery()).isEqualTo("/* Health Check */ SELECT 1");
        assertThat(factory.getValidationQueryTimeout()).isEqualTo(Optional.absent());
    }
}
