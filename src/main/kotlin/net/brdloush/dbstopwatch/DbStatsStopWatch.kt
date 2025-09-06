package net.brdloush.dbstopwatch


import java.text.NumberFormat
import java.util.*
import java.util.concurrent.TimeUnit
import kotlin.math.roundToInt

/**
 * Stop watch that combines timing measurements with P6Spy query statistics.
 *
 * Captures real JDBC execution times including result set processing
 * by leveraging P6Spy event data.
 *
 * Note: This object is not thread-safe and does not use synchronization.
 */


@Suppress("unused")
class DbStatsStopWatch(
    private val id: String = ""
) {

    private var taskList: MutableList<TaskInfo>? = mutableListOf()
    private var startTimeNanos: Long = 0
    private var currentTaskName: String? = null
    private var lastTaskInfo: TaskInfo? = null
    private var taskCount: Int = 0
    private var totalTimeNanos: Long = 0

    /**
     * Configure whether the TaskInfo array is built over time.
     * Set this to false when using for millions of tasks to avoid excessive memory usage.
     */
    fun setKeepTaskList(keepTaskList: Boolean) {
        taskList = if (keepTaskList) mutableListOf() else null
    }

    /**
     * Start an unnamed task.
     */
    fun start(): Unit = start("")

    /**
     * Start a named task.
     * Marks the starting point for P6Spy query capture.
     */
    fun start(taskName: String) {
        check(currentTaskName == null) { "Can't start DbStatsStopWatch: it's already running" }

        currentTaskName = taskName
        startTimeNanos = System.nanoTime()

        // Mark the starting point for P6Spy query capture
        P6SpyStatsCollector.markBlockStart()
    }

    /**
     * Stops previous task (must be running) and starts a new one.
     */
    @SuppressWarnings("unused")
    fun stopAndStart(taskName: String) {
        stop()
        start(taskName)
    }


    /**
     * Stop the current task and capture P6Spy statistics.
     */
    fun stop() {
        check(currentTaskName != null) { "Can't stop DbStatsStopWatch: it's not running" }

        val lastTime = System.nanoTime() - startTimeNanos
        totalTimeNanos += lastTime

        // Get real query statistics from P6Spy
        val blockStats = P6SpyStatsCollector.getBlockStats()

        lastTaskInfo = TaskInfo(
            taskName = currentTaskName!!,
            timeNanos = lastTime,
            queryCount = blockStats.queryCount.toLong(),
            queryMaxMs = blockStats.queryMaxMs.toDouble(),
            queryTotalMs = blockStats.queryTotalMs,
            updateCount = blockStats.updateCount.toLong(),
            updateMaxMs = blockStats.updateMaxMs.toDouble(),
            updateTotalMs = blockStats.updateTotalMs,
        )

        taskList?.add(lastTaskInfo!!)
        taskCount++
        currentTaskName = null
    }

    /**
     * Determine whether this DbStatsStopWatch is currently running.
     */
    @SuppressWarnings("unused")
    fun isRunning(): Boolean = currentTaskName != null

    /**
     * Get the name of the currently running task, if any.
     */
    fun currentTaskName(): String? = currentTaskName

    /**
     * Get the last task as a TaskInfo object.
     */
    fun lastTaskInfo(): TaskInfo {
        return lastTaskInfo ?: error("No tasks run")
    }

    /**
     * Get an array of the data for tasks performed.
     */
    fun getTaskInfo(): Array<TaskInfo> {
        return taskList?.toTypedArray() ?: error("Task info is not being kept!")
    }

    /**
     * Get the number of tasks timed.
     */
    fun getTaskCount(): Int = taskCount

    /**
     * Get the total time for all tasks in nanoseconds.
     */
    fun getTotalTimeNanos(): Long = totalTimeNanos

    /**
     * Get the total time for all tasks in milliseconds.
     */
    fun getTotalTimeMillis(): Long = TimeUnit.NANOSECONDS.toMillis(totalTimeNanos)

    /**
     * Get the total time for all tasks in seconds.
     */
    fun getTotalTimeSeconds(): Double = getTotalTime(TimeUnit.SECONDS)

    /**
     * Get the total time for all tasks in the requested time unit.
     */
    fun getTotalTime(timeUnit: TimeUnit): Double {
        return totalTimeNanos.toDouble() / TimeUnit.NANOSECONDS.convert(1, timeUnit)
    }

    /**
     * Generate a table describing all tasks performed in seconds with query statistics.
     */
    fun prettyPrint(): String = prettyPrint(TimeUnit.SECONDS)


    /**
     * Generate a table describing all tasks performed in the requested time unit with query statistics.
     */
    fun prettyPrint(timeUnit: TimeUnit): String {
        val nf = NumberFormat.getNumberInstance(Locale.ENGLISH).apply {
            maximumFractionDigits = 9
            isGroupingUsed = false
        }

        val pf = NumberFormat.getPercentInstance(Locale.ENGLISH).apply {
            minimumIntegerDigits = 2
            isGroupingUsed = false
        }

        val sb = StringBuilder(256)
        sb.append("DbStatsStopWatch '$id': ")
        val total = if (timeUnit == TimeUnit.NANOSECONDS) {
            nf.format(getTotalTimeNanos())
        } else {
            nf.format(getTotalTime(timeUnit))
        }
        sb.append(total).append(" ${timeUnit.name.lowercase()}")
        val width = maxOf(sb.length, 130)
        sb.append("\n")

        taskList?.let { tasks ->
            val line = "-".repeat(width) + "\n"
            val unitName = timeUnit.name.let {
                it.first() + it.substring(1).lowercase()
            }.padEnd(12)

            sb.append(line)
            sb.append("$unitName  %       Q-cnt    Q-max     Q-total     U-cnt    U-max     U-total     DB%     Task name\n")
            sb.append(line)

            var digits = total.indexOf('.')
            if (digits < 0) digits = total.length
            nf.minimumIntegerDigits = digits
            nf.maximumFractionDigits = 10 - digits

            for (task in tasks) {
                val taskTime = if (timeUnit == TimeUnit.NANOSECONDS) {
                    nf.format(task.timeNanos)
                } else {
                    nf.format(task.getTime(timeUnit))
                }

                val taskPercentage = pf.format(task.getTimeSeconds() / getTotalTimeSeconds())

                val queryCount = task.queryCount
                val (queryMaxTimeDisplay, queryTotalTimeDisplay) = formatTimeDisplay(
                    queryCount, task.queryMaxMs, task.queryTotalMs
                )

                val updateCount = task.updateCount
                val (updateMaxTimeDisplay, updateTotalTimeDisplay) = formatTimeDisplay(
                    updateCount, task.updateMaxMs, task.updateTotalMs
                )

                val dbPercentage = if (updateCount > 0 || queryCount > 0) {
                    val dbPercent = ((task.updateTotalMs.toDouble() + task.queryTotalMs.toDouble()) / task.getTimeMillis()) * 100
                    "%-8.0f".format(dbPercent)
                } else {
                    "%-8s".format("-")
                }

                sb.append("%-14s".format(taskTime))
                sb.append("%-8s".format(taskPercentage))
                sb.append("%-9d".format(queryCount))
                sb.append("%-10s".format(queryMaxTimeDisplay))
                sb.append("%-12s".format(queryTotalTimeDisplay))
                sb.append("%-9d".format(updateCount))
                sb.append("%-10s".format(updateMaxTimeDisplay))
                sb.append("%-12s".format(updateTotalTimeDisplay))
                sb.append(dbPercentage)
                sb.append(task.taskName).append('\n')
            }
        } ?: run {
            sb.append("No task info kept")
        }

        return sb.toString()
    }

    /**
     * Get a short description of the total running time in seconds.
     */
    fun shortSummary(): String = "DbStatsStopWatch '$id': ${getTotalTimeSeconds()} seconds"

    /**
     * Generate an informative string describing all tasks performed in seconds.
     */
    override fun toString(): String {
        val sb = StringBuilder(shortSummary())
        taskList?.let { tasks ->
            for (task in tasks) {
                sb.append("; [${task.taskName}] took ${task.getTimeSeconds()} seconds")
                val percent = (100.0 * task.getTimeSeconds() / getTotalTimeSeconds()).roundToInt()
                sb.append(" = $percent%")
                if (task.queryCount > 0) {
                    val dbPercent = ((task.queryTotalMs.toDouble() / task.getTimeMillis()) * 100).roundToInt()
                    sb.append(" (${task.queryCount} queries, ${task.queryTotalMs}ms total, ${dbPercent}% DB)")
                } else {
                    sb.append(" (no queries)")
                }
            }
        } ?: run {
            sb.append("; no task info kept")
        }
        return sb.toString()
    }

    private fun formatTimeDisplay(
        count: Long,
        maxMs: Double?,
        totalMs: Long?,
        maxWidth: Int = 9,
        totalWidth: Int = 11
    ): Pair<String, String> {
        return if (count > 0) {
            "%-${maxWidth}.0f".format(maxMs) to "%-${totalWidth}s".format(totalMs)
        } else {
            "%-${maxWidth}s".format("-") to "%-${totalWidth}s".format("-")
        }
    }

    /**
     * Data class to hold information about one task including query statistics.
     */
    data class TaskInfo(
        val taskName: String,
        val timeNanos: Long,
        val queryCount: Long,
        val queryMaxMs: Double,
        val queryTotalMs: Long,
        val updateCount: Long,
        val updateMaxMs: Double,
        val updateTotalMs: Long,
    ) {

        /**
         * Get the time this task took in milliseconds.
         */
        fun getTimeMillis(): Long = TimeUnit.NANOSECONDS.toMillis(timeNanos)

        /**
         * Get the time this task took in seconds.
         */
        fun getTimeSeconds(): Double = getTime(TimeUnit.SECONDS)

        /**
         * Get the time this task took in the requested time unit.
         */
        fun getTime(timeUnit: TimeUnit): Double {
            return timeNanos.toDouble() / TimeUnit.NANOSECONDS.convert(1, timeUnit)
        }
    }
}
