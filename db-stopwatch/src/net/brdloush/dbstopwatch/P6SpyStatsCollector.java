package net.brdloush.dbstopwatch;

import com.p6spy.engine.common.PreparedStatementInformation;
import com.p6spy.engine.common.StatementInformation;
import com.p6spy.engine.event.JdbcEventListener;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * P6Spy event listener that captures real JDBC execution times
 * including result set processing time.
 */
public class P6SpyStatsCollector extends JdbcEventListener {

    private static final ThreadLocal<QueryStatsBuffer> threadLocalStats = 
        ThreadLocal.withInitial(QueryStatsBuffer::new);

    public static void markBlockStart() {
        threadLocalStats.get().markBlockStart();
    }

    public static BlockStats getBlockStats() {
        return threadLocalStats.get().getBlockStats();
    }

    public static void reset() {
        threadLocalStats.get().reset();
    }

    @Override
    public void onAfterExecuteQuery(PreparedStatementInformation statementInformation, long timeElapsedNanos, SQLException e) {
        trackExecuteQuery(statementInformation, timeElapsedNanos, statementInformation.getSql());
    }

    @Override
    public void onAfterExecuteQuery(StatementInformation statementInformation, long timeElapsedNanos, String sql, SQLException e) {
        trackExecuteQuery(statementInformation, timeElapsedNanos, sql);
    }

    private void trackExecuteQuery(StatementInformation statementInformation, long timeElapsedNanos, String sql) {
        if (statementInformation != null && statementInformation.getSql() != null) {
            if (!isSystemQuery(sql)) {
                threadLocalStats.get().recordQuery(timeElapsedNanos / 1_000_000, sql);
            }
        }
    }

    @Override
    public void onAfterExecuteUpdate(StatementInformation statementInformation, long timeElapsedNanos, String sql, int rowCount, SQLException e) {
        trackExecuteAndExecuteUpdate(statementInformation, timeElapsedNanos, sql);
    }

    @Override
    public void onAfterExecuteUpdate(PreparedStatementInformation statementInformation, long timeElapsedNanos, int rowCount, SQLException e) {
        trackExecuteAndExecuteUpdate(statementInformation, timeElapsedNanos, statementInformation.getSql());
    }

    @Override
    public void onAfterExecute(PreparedStatementInformation statementInformation, long timeElapsedNanos, SQLException e) {
        trackExecuteAndExecuteUpdate(statementInformation, timeElapsedNanos,  statementInformation.getSql());
    }

    @Override
    public void onAfterExecute(StatementInformation statementInformation, long timeElapsedNanos, String sql, SQLException e) {
        trackExecuteAndExecuteUpdate(statementInformation, timeElapsedNanos,  statementInformation.getSql());
    }

    private void trackExecuteAndExecuteUpdate(StatementInformation statementInformation, long timeElapsedNanos, String sql) {
        if (statementInformation != null && statementInformation.getSql() != null) {
            if (!isSystemQuery(sql)) {
                threadLocalStats.get().recordUpdate(timeElapsedNanos / 1_000_000, sql);
            }
        }
    }

    @Override
    public void onAfterExecuteBatch(StatementInformation statementInformation,
                                   long timeElapsedNanos, int[] updateCounts, SQLException e) {
        if (statementInformation != null && statementInformation.getSql() != null) {
            String sql = statementInformation.getSql();
            if (!isSystemQuery(sql)) {
                threadLocalStats.get().recordBatch(timeElapsedNanos / 1_000_000, sql);
            }
        }
    }

    private boolean isSystemQuery(String sql) {
        String cleanSql = sql.trim().toLowerCase();
        return cleanSql.startsWith("select 1") ||
               cleanSql.contains("information_schema") ||
               cleanSql.contains("sys.") ||
               cleanSql.startsWith("set ");
    }

    private static class QueryStatsBuffer {
        private final List<StatementExecution> queries = new ArrayList<>();
        private final List<StatementExecution> updates = new ArrayList<>();
        private final List<StatementExecution> batches = new ArrayList<>();
        private int blockStartIndexQueries = 0;
        private int blockStartIndexUpdates = 0;
        private int blockStartIndexBatches = 0;

        public void recordQuery(long executionTimeMs, String sql) {
            queries.add(new StatementExecution(executionTimeMs, sql));
        }

        public void recordUpdate(long executionTimeMs, String sql) {
            updates.add(new StatementExecution(executionTimeMs, sql));
        }

        public void recordBatch(long executionTimeMs, String sql) {
            batches.add(new StatementExecution(executionTimeMs, sql));
        }

        public void markBlockStart() {
            blockStartIndexQueries = queries.size();
            blockStartIndexUpdates = updates.size();
            blockStartIndexBatches = batches.size();
        }

        public BlockStats getBlockStats() {
            List<StatementExecution> blockQueries = queries.subList(blockStartIndexQueries, queries.size());
            List<StatementExecution> blockUpdates = updates.subList(blockStartIndexUpdates, updates.size());
            List<StatementExecution> blockBatches = batches.subList(blockStartIndexBatches, batches.size());

            return new BlockStats(
                blockQueries.size(),
                blockQueries.stream().mapToLong(StatementExecution::getExecutionTimeMs).sum(),
                blockQueries.stream().mapToLong(StatementExecution::getExecutionTimeMs).max().orElse(0L),
                blockQueries.stream().map(StatementExecution::getSql).toList(),
                blockUpdates.size(),
                blockUpdates.stream().mapToLong(StatementExecution::getExecutionTimeMs).sum(),
                blockUpdates.stream().mapToLong(StatementExecution::getExecutionTimeMs).max().orElse(0L),
                blockUpdates.stream().map(StatementExecution::getSql).toList(),
                blockBatches.size(),
                blockBatches.stream().mapToLong(StatementExecution::getExecutionTimeMs).sum(),
                blockBatches.stream().mapToLong(StatementExecution::getExecutionTimeMs).max().orElse(0L),
                blockBatches.stream().map(StatementExecution::getSql).toList()
            );
        }

        public void reset() {
            queries.clear();
            updates.clear();
            batches.clear();
            blockStartIndexQueries = 0;
            blockStartIndexUpdates = 0;
            blockStartIndexBatches = 0;
        }
    }

    private static class StatementExecution {
        private final long executionTimeMs;
        private final String sql;

        public StatementExecution(long executionTimeMs, String sql) {
            this.executionTimeMs = executionTimeMs;
            this.sql = sql;
        }

        public long getExecutionTimeMs() {
            return executionTimeMs;
        }

        public String getSql() {
            return sql;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            StatementExecution that = (StatementExecution) o;
            return executionTimeMs == that.executionTimeMs && Objects.equals(sql, that.sql);
        }

        @Override
        public int hashCode() {
            return Objects.hash(executionTimeMs, sql);
        }

        @Override
        public String toString() {
            return "StatementExecution{" +
                   "executionTimeMs=" + executionTimeMs +
                   ", sql='" + sql + '\'' +
                   '}';
        }
    }

    public static class BlockStats {
        private final int queryCount;
        private final long queryTotalMs;
        private final long queryMaxMs;
        private final List<String> querySqls;
        private final int updateCount;
        private final long updateTotalMs;
        private final long updateMaxMs;
        private final List<String> updateSqls;
        private final int batchCount;
        private final long batchTotalMs;
        private final long batchMaxMs;
        private final List<String> batchSqls;

        public BlockStats(int queryCount, long queryTotalMs, long queryMaxMs, List<String> querySqls,
                         int updateCount, long updateTotalMs, long updateMaxMs, List<String> updateSqls,
                         int batchCount, long batchTotalMs, long batchMaxMs, List<String> batchSqls) {
            this.queryCount = queryCount;
            this.queryTotalMs = queryTotalMs;
            this.queryMaxMs = queryMaxMs;
            this.querySqls = querySqls;
            this.updateCount = updateCount;
            this.updateTotalMs = updateTotalMs;
            this.updateMaxMs = updateMaxMs;
            this.updateSqls = updateSqls;
            this.batchCount = batchCount;
            this.batchTotalMs = batchTotalMs;
            this.batchMaxMs = batchMaxMs;
            this.batchSqls = batchSqls;
        }

        public int getQueryCount() { return queryCount; }
        public long getQueryTotalMs() { return queryTotalMs; }
        public double getQueryMaxMs() { return queryMaxMs; }
        public List<String> getQuerySqls() { return querySqls; }
        public int getUpdateCount() { return updateCount; }
        public long getUpdateTotalMs() { return updateTotalMs; }
        public double getUpdateMaxMs() { return updateMaxMs; }
        public List<String> getUpdateSqls() { return updateSqls; }
        public int getBatchCount() { return batchCount; }
        public long getBatchTotalMs() { return batchTotalMs; }
        public double getBatchMaxMs() { return batchMaxMs; }
        public List<String> getBatchSqls() { return batchSqls; }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            BlockStats that = (BlockStats) o;
            return queryCount == that.queryCount &&
                   queryTotalMs == that.queryTotalMs &&
                   queryMaxMs == that.queryMaxMs &&
                   updateCount == that.updateCount &&
                   updateTotalMs == that.updateTotalMs &&
                   updateMaxMs == that.updateMaxMs &&
                   batchCount == that.batchCount &&
                   batchTotalMs == that.batchTotalMs &&
                   batchMaxMs == that.batchMaxMs &&
                   Objects.equals(querySqls, that.querySqls) &&
                   Objects.equals(updateSqls, that.updateSqls) &&
                   Objects.equals(batchSqls, that.batchSqls);
        }

        @Override
        public int hashCode() {
            return Objects.hash(queryCount, queryTotalMs, queryMaxMs, querySqls,
                              updateCount, updateTotalMs, updateMaxMs, updateSqls,
                              batchCount, batchTotalMs, batchMaxMs, batchSqls);
        }

        @Override
        public String toString() {
            return "BlockStats{" +
                   "queryCount=" + queryCount +
                   ", queryTotalMs=" + queryTotalMs +
                   ", queryMaxMs=" + queryMaxMs +
                   ", updateCount=" + updateCount +
                   ", updateTotalMs=" + updateTotalMs +
                   ", updateMaxMs=" + updateMaxMs +
                   ", batchCount=" + batchCount +
                   ", batchTotalMs=" + batchTotalMs +
                   ", batchMaxMs=" + batchMaxMs +
                   '}';
        }
    }
}