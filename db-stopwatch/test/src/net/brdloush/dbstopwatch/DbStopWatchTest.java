package net.brdloush.dbstopwatch;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.jdbc.datasource.SingleConnectionDataSource;

import java.sql.SQLException;
import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

class DbStopWatchTest {

    private final String JDBC_URL = "jdbc:p6spy:h2:mem:testdb;DB_CLOSE_DELAY=-1";
    private Clock mockClock;

    @BeforeEach
    void setUp() {
        mockClock = Mockito.mock();
    }

    @Test
    void noDbOpsApiUsageTest() {
        // given
        var logOutputSb = new StringBuilder();
        var baseInstant = Instant.parse("2025-12-01T12:00:00.00Z");
        when(mockClock.instant()).thenReturn(
                baseInstant.plusSeconds(0), // task 1 start
                baseInstant.plusSeconds(5), // task 1 end (total time: 5s)
                baseInstant.plusSeconds(5), // task 2 start
                baseInstant.plusSeconds(15) // task 2 end (total time: 10s)
        );
        Consumer<String> testLogger = logOutputSb::append;

        // when
        var sw = new DbStopWatch("noDbOpsApiUsageTest", testLogger, () -> mockClock);
        sw.start("task1");
        sw.stopAndStart("task2");
        sw.finish();

        // then
        var logOutput = logOutputSb.toString();
        assertEquals("""
                noDbOpsApiUsageTest / "task1" - START
                noDbOpsApiUsageTest / "task1" - END
                noDbOpsApiUsageTest / "task2" - START
                noDbOpsApiUsageTest / "task2" - END
                DbStopWatch 'noDbOpsApiUsageTest': 15 seconds
                ----------------------------------------------------------------------------------------------------------------------------------
                Seconds       %       Q-cnt    Q-max     Q-total     U-cnt    U-max     U-total     B-cnt    B-max     B-total     DB%     Task name
                ----------------------------------------------------------------------------------------------------------------------------------
                05            33%     0        -         -           0        -         -           0        -         -           -       task1
                10            67%     0        -         -           0        -         -           0        -         -           -       task2
                """, logOutput);
    }

    @Test
    void dbOpsApiUsageTest() {
        // given
        var logOutputSb = new StringBuilder();
        var baseInstant = Instant.parse("2025-12-01T12:00:00.00Z");
        when(mockClock.instant()).thenReturn(
                baseInstant.plusSeconds(0), // task 1 start
                baseInstant.plusSeconds(5), // task 1 end (total time: 5s)
                baseInstant.plusSeconds(5), // task 2 start
                baseInstant.plusSeconds(15) // task 2 end (total time: 10s)
        );
        Consumer<String> testLogger = logOutputSb::append;

        DriverManagerDataSource dataSource = new DriverManagerDataSource();
        dataSource.setDriverClassName("com.p6spy.engine.spy.P6SpyDriver");
        dataSource.setUrl(JDBC_URL);
        dataSource.setUsername("sa");
        dataSource.setPassword("");

        try (var connection = dataSource.getConnection()) {
            var singleConnectionDatasource = new SingleConnectionDataSource(connection, true);
            var jdbc = new JdbcTemplate(singleConnectionDatasource);
            jdbc.execute("""
                         CREATE TABLE users (
                           id BIGINT AUTO_INCREMENT PRIMARY KEY,
                           name VARCHAR(255) NOT NULL,
                           email VARCHAR(255) NOT NULL,
                           active BOOLEAN DEFAULT true,
                           created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                         )
                """);

            // when
            var sw = new DbStopWatch("dbOpsApiUsageTest", testLogger, () -> mockClock);
            sw.start("task1");
            // 3 queries
            jdbc.queryForObject("SELECT COUNT(*) FROM users where id = 1", Integer.class);
            jdbc.queryForObject("SELECT COUNT(*) FROM users where id = 2", Integer.class);
            jdbc.queryForObject("SELECT COUNT(*) FROM users where id = 3", Integer.class);
            // 2 updates
            jdbc.execute("insert into users (name, email) values('foo', 'foo@example.com')");
            jdbc.execute("insert into users (name, email) values('bar', 'bar@example.com')");
            // 1 batch-update
            jdbc.batchUpdate(
                    "INSERT INTO users (name, email) VALUES (?, ?)",
                    List.of(
                            new Object[]{"Alice", "alice@example.com"},
                            new Object[]{"Bob", "bob@example.com"},
                            new Object[]{"Charlie", "charlie@example.com"}
                    )
            );

            sw.finish();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        // then
        var logOutput = logOutputSb.toString();

        var stats = logOutput.lines()
                .filter(line -> line.matches("^\\d+.*"))  // Only data rows starting with digits
                .map(this::parseStats)
                .findFirst()
                .orElseThrow();

        assertEquals(3, stats.qCnt());
        assertEquals(2, stats.uCnt());
        assertEquals(1, stats.bCnt());
    }

    record TaskStats(int qCnt, int uCnt, int bCnt, String taskName) {}

    private TaskStats parseStats(String line) {
        String[] parts = line.trim().split("\\s+");
        return new TaskStats(
                Integer.parseInt(parts[2]),   // Q-cnt
                Integer.parseInt(parts[5]),   // U-cnt
                Integer.parseInt(parts[8]),   // B-cnt
                parts[12]                     // Task name
        );
    }
}