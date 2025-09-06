# DB Stopwatch

A Kotlin library (also usable from java applications) that combines precise timing measurements with comprehensive database query statistics, built on top of P6Spy. Perfect for performance monitoring, optimization, and debugging database-heavy applications.

## Features

- 🕐 **Precise Timing**: Captures real JDBC execution times including result set processing
- 📊 **Comprehensive Stats**: Tracks query counts, execution times, and percentages
- 🔍 **Smart Filtering**: Automatically excludes system queries and metadata calls
- 🧵 **Thread-Safe**: Uses ThreadLocal storage for concurrent environments
- 📝 **Rich Reporting**: Beautiful ASCII tables and summary reports
- 🚀 **Zero Overhead**: Minimal performance impact when not actively measuring
- ⚡ **Easy Integration**: Simple start/stop API with Spring-like interface

## Quick Start

### 1. Add Dependencies

Add to your `pom.xml`:

```xml
<dependencies>
    <dependency>
        <groupId>net.brdloush</groupId>
        <artifactId>db-stopwatch</artifactId>
        <version>1.0.0</version>
    </dependency>
    <dependency>
        <groupId>p6spy</groupId>
        <artifactId>p6spy</artifactId>
        <version>3.9.1</version>
    </dependency>
</dependencies>
```

### 2. Configure P6Spy

Update your JDBC URL to use P6Spy:
```
# Before
jdbc:postgresql://localhost:5432/mydb

# After  
jdbc:p6spy:postgresql://localhost:5432/mydb
```

Optional: Create `src/main/resources/spy.properties` for P6Spy configuration:
```properties
driverlist=org.postgresql.Driver
logMessageFormat=com.p6spy.engine.spy.appender.CustomLineFormat
customLogMessageFormat=%(executionTime)|%(category)|%(sql)
appender=com.p6spy.engine.spy.appender.Slf4JLogger
excludecategories=info,debug,result,resultset,batch
filter=false
```

### 3. Start Measuring

```kotlin
import net.brdloush.dbstopwatch.DbStatsStopWatch

fun processOrders() {
    val stopwatch = DbStatsStopWatch("Order Processing")
    
    stopwatch.start("Loading pending orders")
    val orders = orderRepository.findPendingOrders()
    
    stopwatch.stopAndStart("Validating orders")
    val validOrders = orders.filter { validateOrder(it) }
    
    stopwatch.stopAndStart("Updating order status")
    validOrders.forEach { orderRepository.updateStatus(it.id, PROCESSED) }
    
    stopwatch.stop()
    
    // Beautiful performance report
    logger.info("\n${stopwatch.prettyPrint()}")
}
```

## Example Output

```
DbStatsStopWatch 'Order Processing': 0.847 seconds
-----------------------------------------------------------------------------------------------------------------------------------
Seconds       %       Q-cnt    Q-max     Q-total     U-cnt    U-max     U-total     DB%     Task name
-----------------------------------------------------------------------------------------------------------------------------------
0.312         37%     15       23        156         0        -         -           50      Loading pending orders
0.089         11%     0        -         -           0        -         -           -       Validating orders  
0.446         53%     5        89        234         12       45        198         97      Updating order status
```

## API Reference

### DbStatsStopWatch

Main class for performance monitoring with database statistics.

#### Constructor
```kotlin
DbStatsStopWatch(id: String = "")
```

#### Core Methods

| Method | Description |
|--------|-------------|
| `start(taskName: String = "")` | Start timing a named task |
| `stop()` | Stop current task and capture statistics |
| `stopAndStart(taskName: String)` | Stop current task and immediately start a new one |
| `isRunning(): Boolean` | Check if stopwatch is currently running |
| `currentTaskName(): String?` | Get name of currently running task |

#### Results & Reporting

| Method | Description |
|--------|-------------|
| `prettyPrint(): String` | Generate detailed ASCII table report |
| `prettyPrint(timeUnit: TimeUnit): String` | Report in specific time unit |
| `shortSummary(): String` | Brief one-line summary |
| `toString(): String` | Detailed text summary |
| `lastTaskInfo(): TaskInfo` | Get details of last completed task |
| `getTaskInfo(): Array<TaskInfo>` | Get all task details |

## Requirements

- **Java**: 8 or higher
- **Kotlin**: 1.8 or higher  
- **P6Spy**: 3.9.1 or higher

## Database Support

Works with any database supported by P6Spy, including:
- PostgreSQL
- MySQL/MariaDB  
- SQL Server
- Oracle
- H2, HSQLDB
- SQLite

## Performance Notes

- **Minimal Overhead**: When not actively measuring, impact is negligible
- **Memory Usage**: Default behavior stores all task details; use `setKeepTaskList(false)` for high-volume scenarios
- **Thread Safety**: Each thread maintains separate statistics via ThreadLocal storage
- **P6Spy Integration**: Leverages P6Spy's efficient event system for accurate timing

## Contributing

Contributions are welcome! Please feel free to submit issues, feature requests, or pull requests.

## License

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) for details.
