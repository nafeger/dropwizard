package io.dropwizard.db;

import com.codahale.metrics.MetricRegistry;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.logging.Logger;

/**
 * A {@link ManagedDataSource} which is backed by a Tomcat pooled {@link javax.sql.DataSource}.
 */
public class ManagedPooledDataSource implements ManagedDataSource {
    private HikariConfig config;
    private final MetricRegistry metricRegistry;
    private HikariDataSource datasource;

    /**
     * Create a new data source with the given connection pool configuration.
     *
     * @param config the connection pool configuration
     */
    public ManagedPooledDataSource(HikariConfig config, MetricRegistry metricRegistry) {
        this.config = config;

        this.metricRegistry = metricRegistry;
    }

    @Override
    public PrintWriter getLogWriter() throws SQLException {
        return this.datasource.getLogWriter();
    }

    @Override
    public void setLogWriter(PrintWriter out) throws SQLException {
       this.datasource.setLogWriter(out);
    }

    @Override
    public void setLoginTimeout(int seconds) throws SQLException {
        this.datasource.setLoginTimeout(seconds);
    }

    @Override
    public int getLoginTimeout() throws SQLException {
        return this.datasource.getLoginTimeout();
    }

    // JDK6 has JDBC 4.0 which doesn't have this -- don't add @Override
    @SuppressWarnings("override")
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("Doesn't use java.util.logging");
    }

    @Override
    public void start() throws Exception {
        datasource = new HikariDataSource(this.config);
        datasource.setMetricRegistry(metricRegistry);
    }

    @Override
    public void stop() throws Exception {
        datasource.close();
    }

    @Override
    public Connection getConnection() throws SQLException {
        return this.datasource.getConnection();
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException {
        return this.datasource.getConnection(username,password);
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return this.datasource.unwrap(iface);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return this.datasource.isWrapperFor(iface);
    }
}
