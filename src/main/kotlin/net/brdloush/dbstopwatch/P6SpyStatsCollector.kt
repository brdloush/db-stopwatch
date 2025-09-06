package net.brdloush.dbstopwatch

import com.p6spy.engine.common.PreparedStatementInformation
import com.p6spy.engine.event.JdbcEventListener

import java.sql.SQLException

/**
 * P6Spy event listener that captures real JDBC execution times
 * including result set processing time.
 */
class P6SpyStatsCollector : JdbcEventListener() {

    companion object {
        private val threadLocalStats = ThreadLocal.withInitial { QueryStatsBuffer() }

        fun markBlockStart() {
            threadLocalStats.get().markBlockStart()
        }

        fun getBlockStats(): BlockStats {
            return threadLocalStats.get().getBlockStats()
        }

        fun reset() {
            threadLocalStats.get().reset()
        }
    }

    override fun onAfterExecuteQuery(
        statementInformation: PreparedStatementInformation?,
        timeElapsedNanos: Long,
        e: SQLException?
    ) {
        val sql = statementInformation!!.sql
        if (sql != null && !isSystemQuery(sql)) {
            threadLocalStats.get().recordQuery(timeElapsedNanos / 1_000_000, sql)
        }
    }

    override fun onAfterExecuteUpdate(
        statementInformation: PreparedStatementInformation?,
        timeElapsedNanos: Long,
        rowCount: Int,
        e: SQLException?
    ) {
        val sql = statementInformation!!.sql
        if (sql != null && !isSystemQuery(sql)) {
            threadLocalStats.get().recordUpdate(timeElapsedNanos / 1_000_000, sql)
        }
    }

    private fun isSystemQuery(sql: String): Boolean {
        val cleanSql = sql.trim().lowercase()
        return cleanSql.startsWith("select 1") ||
                cleanSql.contains("information_schema") ||
                cleanSql.contains("sys.") ||
                cleanSql.startsWith("set ")
    }

    private class QueryStatsBuffer {
        private val queries = mutableListOf<StatementExecution>()
        private val updates = mutableListOf<StatementExecution>()
        private var blockStartIndexQueries = 0
        private var blockStartIndexUpdates = 0

        fun recordQuery(executionTimeMs: Long, sql: String) {
            queries.add(StatementExecution(executionTimeMs, sql))
        }

        fun recordUpdate(executionTimeMs: Long, sql: String) {
            updates.add(StatementExecution(executionTimeMs, sql))
        }

        fun markBlockStart() {
            blockStartIndexQueries = queries.size
            blockStartIndexUpdates = updates.size
        }

        fun getBlockStats(): BlockStats {
            val blockQueries = queries.subList(blockStartIndexQueries, queries.size)
            val blockUpdates = updates.subList(blockStartIndexUpdates, updates.size)
            return BlockStats(
                queryCount = blockQueries.size,
                queryTotalMs = blockQueries.sumOf { it.executionTimeMs },
                queryMaxMs = blockQueries.maxOfOrNull { it.executionTimeMs } ?: 0L,
                querySqls = blockQueries.map { it.sql },
                updateCount = blockUpdates.size,
                updateTotalMs = blockUpdates.sumOf { it.executionTimeMs },
                updateMaxMs = blockUpdates.maxOfOrNull { it.executionTimeMs } ?: 0L,
                updateSqls = blockUpdates.map { it.sql },
            )
        }

        fun reset() {
            queries.clear()
            updates.clear()
            blockStartIndexQueries = 0
        }
    }

    private data class StatementExecution(
        val executionTimeMs: Long,
        val sql: String
    )

    data class BlockStats(
        val queryCount: Int,
        val queryTotalMs: Long,
        val queryMaxMs: Long,
        val querySqls: List<String>,
        val updateCount: Int,
        val updateTotalMs: Long,
        val updateMaxMs: Long,
        val updateSqls: List<String>
    )
}
