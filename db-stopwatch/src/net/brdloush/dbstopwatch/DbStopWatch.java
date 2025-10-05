package net.brdloush.dbstopwatch;

import java.text.NumberFormat;
import java.time.Clock;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Stop watch that combines timing measurements with P6Spy query statistics.
 * <p>
 * Captures real JDBC execution times including result set processing
 * by leveraging P6Spy event data.
 * <p>
 * Note: This object is not thread-safe and does not use synchronization.
 */
public class DbStopWatch {
    private final String id;
    private final Consumer<String> logFunction;
    private final Clock clock;
    private List<TaskInfo> taskList = new ArrayList<>();
    private long startTimeNanos = 0;
    private String currentTaskName = null;
    private TaskInfo lastTaskInfo = null;
    private int taskCount = 0;
    private long totalTimeNanos = 0;

    public DbStopWatch() {
        this("", null, null);
    }

    public DbStopWatch(String id) {
        this(id, null, null);
    }

    public DbStopWatch(String id, Consumer<String> logFunction) {
        this(id, logFunction, null);
    }

    public DbStopWatch(String id, Consumer<String> logFunction, Supplier<Clock> customClockSupplier) {
        this.id = id;
        this.logFunction = logFunction;
        this.clock = customClockSupplier != null ? customClockSupplier.get() : Clock.systemUTC();
    }

    /**
     * Configure whether the TaskInfo array is built over time.
     * Set this to false when using for millions of tasks to avoid excessive memory usage.
     */
    public void setKeepTaskList(boolean keepTaskList) {
        this.taskList = keepTaskList ? new ArrayList<>() : null;
    }

    /**
     * Start an unnamed task.
     */
    public void start() {
        start("");
    }

    /**
     * Start a named task.
     * Marks the starting point for P6Spy query capture.
     */
    public void start(String taskName) {
        if (logFunction != null) {
            logFunction.accept(id + " / \"" + taskName + "\" - START\n");
        }

        if (currentTaskName != null) {
            throw new IllegalStateException("Can't start DbStopWatch: it's already running");
        }

        this.currentTaskName = taskName;
        this.startTimeNanos = epochNanoFromClock();

        // Mark the starting point for P6Spy query capture
        P6SpyStatsCollector.markBlockStart();
    }

    private long epochNanoFromClock() {
        var now = clock.instant();
        long seconds = now.getEpochSecond();
        int nanos = now.getNano();
        return seconds * 1_000_000_000L + nanos;
    }

    /**
     * Stops previous task (must be running) and starts a new one.
     */
    public void stopAndStart(String taskName) {
        stop();
        start(taskName);
    }

    /**
     * Stops previous task (if running) and pretty prints to logFunction (if specified)
     */
    public void finish() {
        if (isRunning()) {
            stop();
        }
        if (logFunction != null) {
            logFunction.accept(prettyPrint());
        }
    }

    /**
     * Stop the current task and capture P6Spy statistics.
     */
    public void stop() {
        if (currentTaskName == null) {
            throw new IllegalStateException("Can't stop DbStopWatch: it's not running");
        }

        if (logFunction != null) {
            logFunction.accept(id + " / \"" + currentTaskName + "\" - END\n");
        }

        long lastTime = epochNanoFromClock() - startTimeNanos;
        totalTimeNanos += lastTime;

        // Get real query statistics from P6Spy
        P6SpyStatsCollector.BlockStats blockStats = P6SpyStatsCollector.getBlockStats();

        lastTaskInfo = new TaskInfo(
            currentTaskName,
            lastTime,
            blockStats.getQueryCount(),
            blockStats.getQueryMaxMs(),
            blockStats.getQueryTotalMs(),
            blockStats.getUpdateCount(),
            blockStats.getUpdateMaxMs(),
            blockStats.getUpdateTotalMs(),
            blockStats.getBatchCount(),
            blockStats.getBatchMaxMs(),
            blockStats.getBatchTotalMs()
        );

        if (taskList != null) {
            taskList.add(lastTaskInfo);
        }
        taskCount++;
        currentTaskName = null;
    }

    /**
     * Determine whether this DbStopWatch is currently running.
     */
    public boolean isRunning() {
        return currentTaskName != null;
    }

    /**
     * Get the name of the currently running task, if any.
     */
    public String getCurrentTaskName() {
        return currentTaskName;
    }

    /**
     * Get the last task as a TaskInfo object.
     */
    public TaskInfo getLastTaskInfo() {
        if (lastTaskInfo == null) {
            throw new IllegalStateException("No tasks run");
        }
        return lastTaskInfo;
    }

    /**
     * Get an array of the data for tasks performed.
     */
    public TaskInfo[] getTaskInfo() {
        if (taskList == null) {
            throw new IllegalStateException("Task info is not being kept!");
        }
        return taskList.toArray(new TaskInfo[0]);
    }

    /**
     * Get the number of tasks timed.
     */
    public int getTaskCount() {
        return taskCount;
    }

    /**
     * Get the total time for all tasks in nanoseconds.
     */
    public long getTotalTimeNanos() {
        return totalTimeNanos;
    }

    /**
     * Get the total time for all tasks in milliseconds.
     */
    public long getTotalTimeMillis() {
        return TimeUnit.NANOSECONDS.toMillis(totalTimeNanos);
    }

    /**
     * Get the total time for all tasks in seconds.
     */
    public double getTotalTimeSeconds() {
        return getTotalTime(TimeUnit.SECONDS);
    }

    /**
     * Get the total time for all tasks in the requested time unit.
     */
    public double getTotalTime(TimeUnit timeUnit) {
        return (double) totalTimeNanos / TimeUnit.NANOSECONDS.convert(1, timeUnit);
    }

    /**
     * Generate a table describing all tasks performed in seconds with query statistics.
     */
    public String prettyPrint() {
        return prettyPrint(TimeUnit.SECONDS);
    }

    /**
     * Generate a table describing all tasks performed in the requested time unit with query statistics.
     */
    public String prettyPrint(TimeUnit timeUnit) {
        NumberFormat nf = NumberFormat.getNumberInstance(Locale.ENGLISH);
        nf.setMaximumFractionDigits(9);
        nf.setGroupingUsed(false);

        NumberFormat pf = NumberFormat.getPercentInstance(Locale.ENGLISH);
        pf.setMinimumIntegerDigits(2);
        pf.setGroupingUsed(false);

        StringBuilder sb = new StringBuilder(256);
        sb.append("DbStopWatch '").append(id).append("': ");
        
        String total;
        if (timeUnit == TimeUnit.NANOSECONDS) {
            total = nf.format(getTotalTimeNanos());
        } else {
            total = nf.format(getTotalTime(timeUnit));
        }
        sb.append(total).append(" ").append(timeUnit.name().toLowerCase());
        int width = Math.max(sb.length(), 130);
        sb.append("\n");

        if (taskList != null) {
            String line = "-".repeat(width) + "\n";
            String unitName = timeUnit.name();
            unitName = unitName.charAt(0) + unitName.substring(1).toLowerCase();
            unitName = String.format("%-12s", unitName);

            sb.append(line);
            sb.append(unitName).append("  %       Q-cnt    Q-max     Q-total     U-cnt    U-max     U-total     B-cnt    B-max     B-total     DB%     Task name\n");
            sb.append(line);

            int digits = total.indexOf('.');
            if (digits < 0) digits = total.length();
            nf.setMinimumIntegerDigits(digits);
            nf.setMaximumFractionDigits(10 - digits);

            for (TaskInfo task : taskList) {
                String taskTime;
                if (timeUnit == TimeUnit.NANOSECONDS) {
                    taskTime = nf.format(task.getTimeNanos());
                } else {
                    taskTime = nf.format(task.getTime(timeUnit));
                }

                String taskPercentage = pf.format((double) task.getTimeMillis() / getTotalTimeMillis());

                long queryCount = task.getQueryCount();
                String[] queryTimes = formatTimeDisplay(queryCount, task.getQueryMaxMs(), task.getQueryTotalMs());

                long updateCount = task.getUpdateCount();
                String[] updateTimes = formatTimeDisplay(updateCount, task.getUpdateMaxMs(), task.getUpdateTotalMs());

                long batchCount = task.getBatchCount();
                String[] batchTimes = formatTimeDisplay(batchCount, task.getBatchMaxMs(), task.getBatchTotalMs());

                String dbPercentage;
                if (updateCount > 0 || queryCount > 0 || batchCount > 0) {
                    double dbPercent = ((task.getUpdateTotalMs() + task.getQueryTotalMs() + task.getBatchTotalMs()) / (double) task.getTimeMillis()) * 100;
                    dbPercentage = String.format("%-8.0f", dbPercent);
                } else {
                    dbPercentage = String.format("%-8s", "-");
                }

                sb.append(String.format("%-14s", taskTime));
                sb.append(String.format("%-8s", taskPercentage));
                sb.append(String.format("%-9d", queryCount));
                sb.append(String.format("%-10s", queryTimes[0]));
                sb.append(String.format("%-12s", queryTimes[1]));
                sb.append(String.format("%-9d", updateCount));
                sb.append(String.format("%-10s", updateTimes[0]));
                sb.append(String.format("%-12s", updateTimes[1]));
                sb.append(String.format("%-9d", batchCount));
                sb.append(String.format("%-10s", batchTimes[0]));
                sb.append(String.format("%-12s", batchTimes[1]));
                sb.append(dbPercentage);
                sb.append(task.getTaskName()).append('\n');
            }
        } else {
            sb.append("No task info kept");
        }

        return sb.toString();
    }

    /**
     * Get a short description of the total running time in seconds.
     */
    public String shortSummary() {
        return "DbStopWatch '" + id + "': " + getTotalTimeSeconds() + " seconds";
    }

    /**
     * Generate an informative string describing all tasks performed in seconds.
     */
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(shortSummary());
        if (taskList != null) {
            for (TaskInfo task : taskList) {
                sb.append("; [").append(task.getTaskName()).append("] took ")
                  .append(task.getTimeSeconds()).append(" seconds");
                int percent = (int) Math.round(100.0 * task.getTimeSeconds() / getTotalTimeSeconds());
                sb.append(" = ").append(percent).append("%");
                if (task.getQueryCount() > 0) {
                    int dbPercent = (int) Math.round((task.getQueryTotalMs() / (double) task.getTimeMillis()) * 100);
                    sb.append(" (").append(task.getQueryCount()).append(" queries, ")
                      .append(task.getQueryTotalMs()).append("ms total, ")
                      .append(dbPercent).append("% DB)");
                } else {
                    sb.append(" (no queries)");
                }
            }
        } else {
            sb.append("; no task info kept");
        }
        return sb.toString();
    }

    private String[] formatTimeDisplay(long count, double maxMs, long totalMs) {
        if (count > 0) {
            return new String[]{
                String.format("%-9.0f", maxMs),
                String.format("%-11s", totalMs)
            };
        } else {
            return new String[]{
                String.format("%-9s", "-"),
                String.format("%-11s", "-")
            };
        }
    }

    /**
     * Data class to hold information about one task including query statistics.
     */
    public static class TaskInfo {
        private final String taskName;
        private final long timeNanos;
        private final long queryCount;
        private final double queryMaxMs;
        private final long queryTotalMs;
        private final long updateCount;
        private final double updateMaxMs;
        private final long updateTotalMs;
        private final long batchCount;
        private final double batchMaxMs;
        private final long batchTotalMs;

        public TaskInfo(String taskName, long timeNanos, long queryCount, double queryMaxMs, 
                       long queryTotalMs, long updateCount, double updateMaxMs, long updateTotalMs,
                       long batchCount, double batchMaxMs, long batchTotalMs) {
            this.taskName = taskName;
            this.timeNanos = timeNanos;
            this.queryCount = queryCount;
            this.queryMaxMs = queryMaxMs;
            this.queryTotalMs = queryTotalMs;
            this.updateCount = updateCount;
            this.updateMaxMs = updateMaxMs;
            this.updateTotalMs = updateTotalMs;
            this.batchCount = batchCount;
            this.batchMaxMs = batchMaxMs;
            this.batchTotalMs = batchTotalMs;
        }

        public String getTaskName() { return taskName; }
        public long getTimeNanos() { return timeNanos; }
        public long getQueryCount() { return queryCount; }
        public double getQueryMaxMs() { return queryMaxMs; }
        public long getQueryTotalMs() { return queryTotalMs; }
        public long getUpdateCount() { return updateCount; }
        public double getUpdateMaxMs() { return updateMaxMs; }
        public long getUpdateTotalMs() { return updateTotalMs; }
        public long getBatchCount() { return batchCount; }
        public double getBatchMaxMs() { return batchMaxMs; }
        public long getBatchTotalMs() { return batchTotalMs; }

        /**
         * Get the time this task took in milliseconds.
         */
        public long getTimeMillis() {
            return TimeUnit.NANOSECONDS.toMillis(timeNanos);
        }

        /**
         * Get the time this task took in seconds.
         */
        public double getTimeSeconds() {
            return getTime(TimeUnit.SECONDS);
        }

        /**
         * Get the time this task took in the requested time unit.
         */
        public double getTime(TimeUnit timeUnit) {
            return (double) timeNanos / TimeUnit.NANOSECONDS.convert(1, timeUnit);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            TaskInfo taskInfo = (TaskInfo) o;
            return timeNanos == taskInfo.timeNanos &&
                   queryCount == taskInfo.queryCount &&
                   Double.compare(taskInfo.queryMaxMs, queryMaxMs) == 0 &&
                   queryTotalMs == taskInfo.queryTotalMs &&
                   updateCount == taskInfo.updateCount &&
                   Double.compare(taskInfo.updateMaxMs, updateMaxMs) == 0 &&
                   updateTotalMs == taskInfo.updateTotalMs &&
                   batchCount == taskInfo.batchCount &&
                   Double.compare(taskInfo.batchMaxMs, batchMaxMs) == 0 &&
                   batchTotalMs == taskInfo.batchTotalMs &&
                   Objects.equals(taskName, taskInfo.taskName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(taskName, timeNanos, queryCount, queryMaxMs, queryTotalMs,
                              updateCount, updateMaxMs, updateTotalMs, batchCount, batchMaxMs, batchTotalMs);
        }

        @Override
        public String toString() {
            return "TaskInfo{" +
                   "taskName='" + taskName + '\'' +
                   ", timeNanos=" + timeNanos +
                   ", queryCount=" + queryCount +
                   ", queryMaxMs=" + queryMaxMs +
                   ", queryTotalMs=" + queryTotalMs +
                   ", updateCount=" + updateCount +
                   ", updateMaxMs=" + updateMaxMs +
                   ", updateTotalMs=" + updateTotalMs +
                   ", batchCount=" + batchCount +
                   ", batchMaxMs=" + batchMaxMs +
                   ", batchTotalMs=" + batchTotalMs +
                   '}';
        }
    }
}