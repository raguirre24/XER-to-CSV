# Streaming/Chunked Processing Strategy

## Understanding Your Valid Concerns 🎯

You're absolutely right to be concerned! Your code has **cross-table and cross-file dependencies** that make naive chunking impossible. Let me break down exactly what CAN and CANNOT be chunked.

---

## Current Dependency Analysis

### The Composite Key Pattern (Critical!)

Your code uses this pattern throughout:
```csharp
// Program.cs:794-798
private static string CreateKey(string filename, string value)
{
    string key = $"{filename.Trim()}.{value.Trim()}";
    return StringInternPool.Intern(key);
}
```

**Purpose:** Distinguish records from different XER files
- Task ID 100 in `file1.xer` → Key: `"file1.xer.100"`
- Task ID 100 in `file2.xer` → Key: `"file2.xer.100"`

This allows merging multiple XER files while maintaining relationships!

---

## Dependency Graph Analysis

### 1. Create01XerTaskTable (TASK transformation)

**Dependencies:**
```
TASK table (source - can chunk?)
   ↓
   Lookups needed for EACH task row:
   ├─ CALENDAR table → calendarHours lookup (Program.cs:835, 909-910)
   │  └─ Key: CreateKey(filename, clndr_id)
   │  └─ Must match SAME file as task
   └─ PROJECT table → projectDataDates lookup (Program.cs:836, 946-947)
      └─ Key: CreateKey(filename, proj_id)
      └─ Must match SAME file as task
```

**Current Code (Program.cs:871-972):**
```csharp
// Build lookups ONCE for all calendars and projects
var calendarHours = BuildCalendarHoursLookup(calendarTable);      // Full CALENDAR table
var projectDataDates = BuildProjectDataDatesLookup(projectTable); // Full PROJECT table

// Then process TASK rows in parallel
Parallel.ForEach(taskTable.Rows, parallelOptions, sourceRowData =>
{
    string originalFilename = sourceRowData.SourceFilename;
    string clndrId = GetFieldValue(row, taskIndexes, FieldNames.CalendarId);

    // Lookup using composite key (filename + id)
    string calendarKey = CreateKey(originalFilename, clndrId);
    if (calendarHours.TryGetValue(calendarKey, out decimal dayHrCnt))
    {
        // Use calendar data
    }
});
```

**Key Insight:**
- ✅ **TASK table CAN be chunked** (it's the source being transformed)
- ❌ **CALENDAR table CANNOT be chunked** (it's a lookup table)
- ❌ **PROJECT table CANNOT be chunked** (it's a lookup table)

---

### 2. Create06XerPredecessor (TASKPRED transformation)

**Dependencies:**
```
TASKPRED table (source - can chunk?)
   ↓
   Lookups needed for EACH predecessor row:
   ├─ TASK table → taskLookup (Program.cs:1496, 1525-1527)
   │  └─ Key: CreateKey(filename, task_id) and CreateKey(filename, pred_task_id)
   │  └─ Both successor AND predecessor tasks must be in lookup
   │  └─ Can be from DIFFERENT files!
   ├─ CALENDAR table → calendarHoursLookup (Program.cs:1497, 1543-1545)
   │  └─ Key: CreateKey(filename, clndr_id)
   └─ CALENDAR_DETAILED table → calendarCalculators (Program.cs:1498)
      └─ Used for Free Float working day calculations
```

**Current Code (Program.cs:1505-1582):**
```csharp
// Build lookups ONCE for all tasks, calendars, calendar details
var taskLookup = BuildTaskLookupDictionary(taskTable);           // Full TASK table
var calendarHoursLookup = BuildCalendarHoursLookup(calendarTable);
var calendarCalculators = BuildCalendarCalculators(calendarDetailedTable);

// Then process TASKPRED rows in parallel
Parallel.ForEach(taskPredTable.Rows, parallelOptions, sourceRow =>
{
    string taskIdKey = CreateKey(originalFilename, taskId);
    string predTaskIdKey = CreateKey(originalFilename, predTaskId);

    // Lookup successor task (could be in different file!)
    TaskData succTask = taskLookup.TryGetValue(taskIdKey, out var st) ? st : new TaskData();
    // Lookup predecessor task (could be in different file!)
    TaskData predTask = taskLookup.TryGetValue(predTaskIdKey, out var pt) ? pt : new TaskData();
});
```

**Key Insight:**
- ✅ **TASKPRED table CAN be chunked** (it's the source being transformed)
- ❌ **TASK table CANNOT be chunked when used as lookup** (predecessor relationships can cross files)
- ❌ **CALENDAR and CALENDAR_DETAILED CANNOT be chunked**

---

## What CAN Be Chunked (Safely)

### ✅ Level 1: CSV Export (Already Safe to Chunk)

**Current bottleneck:** `CsvExporter.WriteTableToCsv` (Program.cs:615-674)

**What happens now:**
```csharp
foreach (var dataRow in table.Rows)  // Iterates ALL rows in memory
{
    // Build CSV line
    writer.WriteLine(lineBuilder.ToString());
}
```

**Streaming solution:**
```csharp
// Add to XerTable class
public IEnumerable<IEnumerable<DataRow>> GetRowsInChunks(int chunkSize = 10000)
{
    for (int i = 0; i < _rows.Count; i += chunkSize)
    {
        yield return _rows.Skip(i).Take(chunkSize);
    }
}

// Modify CsvExporter
public void WriteTableToCsvStreaming(XerTable table, string csvFilePath)
{
    using (var writer = new StreamWriter(...))
    {
        // Write headers once
        writer.WriteLine(headerLine);

        // Process rows in chunks
        foreach (var chunk in table.GetRowsInChunks(10000))
        {
            foreach (var row in chunk)
            {
                // Build and write line
                writer.WriteLine(BuildCsvLine(row));
            }
            writer.Flush(); // Flush each chunk
        }
    }
}
```

**Benefits:**
- Reduces memory during export by ~50-70%
- No impact on correctness (export has no dependencies)
- Easy to implement (1-2 days)

**Memory Impact:**
- Before: Hold entire table in memory during export
- After: Only 10,000 rows in memory at a time

---

### ✅ Level 2: Source Table Transformation (With Full Lookup Tables)

**Strategy:** Keep lookup tables in memory, chunk source tables

**Example for Create01XerTaskTable:**

```csharp
public XerTable Create01XerTaskTableStreaming()
{
    var taskTable = _dataStore.GetTable(TableNames.Task);
    var calendarTable = _dataStore.GetTable(TableNames.Calendar);
    var projectTable = _dataStore.GetTable(TableNames.Project);

    // Build lookups ONCE - these MUST stay in memory (small tables)
    var calendarHours = BuildCalendarHoursLookup(calendarTable);
    var projectDataDates = BuildProjectDataDatesLookup(projectTable);

    var finalTable = new XerTable(EnhancedTableNames.XerTask01);
    finalTable.SetHeaders(finalColumns);

    // Process TASK table in chunks
    int chunkSize = 50000; // Process 50K tasks at a time
    for (int offset = 0; offset < taskTable.RowCount; offset += chunkSize)
    {
        var chunk = taskTable.Rows.Skip(offset).Take(chunkSize);
        var transformedChunk = new ConcurrentBag<DataRow>();

        Parallel.ForEach(chunk, parallelOptions, sourceRowData =>
        {
            // Same transformation logic as before
            // Lookups still work because they're in memory
            string calendarKey = CreateKey(originalFilename, clndrId);
            if (calendarHours.TryGetValue(calendarKey, out decimal dayHrCnt))
            {
                // Transform using calendar data
            }
            transformedChunk.Add(transformedRow);
        });

        // Add chunk to final table
        finalTable.AddRows(transformedChunk);

        // GC can collect the processed chunk
        transformedChunk = null;
    }

    return finalTable;
}
```

**Why This Works:**
- Lookup tables (CALENDAR, PROJECT) are typically SMALL (10-100 rows)
- Source table (TASK) is LARGE (100K-1M+ rows)
- Composite keys ensure correct cross-file references
- Memory usage reduced by chunking the large table only

**Memory Impact:**
```
Before:
- TASK: 500K rows × 50 fields × 50 bytes = 1.25 GB
- Transformed: 500K rows × 35 fields × 50 bytes = 875 MB
- TOTAL: ~2.1 GB peak

After (50K row chunks):
- TASK (in chunks): 50K rows × 50 fields × 50 bytes = 125 MB
- Transformed (per chunk): 50K rows × 35 fields × 50 bytes = 87 MB
- Lookup tables: ~10 MB
- TOTAL: ~220 MB peak (90% reduction!)
```

---

### ❌ Level 3: What CANNOT Be Chunked

#### ❌ Lookup Tables (Small but Critical)

These tables MUST remain fully in memory:

1. **CALENDAR table**
   - Used by: Task transformation, Predecessor transformation
   - Why: Tasks look up their calendar using composite key
   - Typical size: 10-50 rows (very small)
   - Memory: ~1-5 MB

2. **PROJECT table**
   - Used by: Task transformation
   - Why: Tasks look up project data date
   - Typical size: 1-20 rows (very small)
   - Memory: <1 MB

3. **TASK table (when used as lookup)**
   - Used by: Predecessor transformation
   - Why: Predecessors look up BOTH successor AND predecessor task data
   - Cross-file lookups: Task in file1.xer can be predecessor to task in file2.xer
   - Typical size: 100K-1M rows (LARGE)
   - Memory: **100 MB - 1 GB (This is the problem!)**

4. **CALENDAR_DETAILED table**
   - Used by: Predecessor Free Float calculation
   - Why: Working day calculators need full calendar definitions
   - Typical size: 100-1000 rows (small-medium)
   - Memory: ~10-50 MB

---

## The Real Problem: TASK Lookup in Predecessor Transformation

### Why TASK Table Can't Be Chunked for Predecessor

**The Issue (Program.cs:1496, 1525-1527):**

```csharp
var taskLookup = BuildTaskLookupDictionary(taskTable); // Full TASK table in memory!

Parallel.ForEach(taskPredTable.Rows, parallelOptions, sourceRow =>
{
    string taskIdKey = CreateKey(originalFilename, taskId);
    string predTaskIdKey = CreateKey(originalFilename, predTaskId);

    // PROBLEM: Both keys could be in DIFFERENT chunks!
    TaskData succTask = taskLookup.TryGetValue(taskIdKey, out var st) ? st : new TaskData();
    TaskData predTask = taskLookup.TryGetValue(predTaskIdKey, out var pt) ? pt : new TaskData();
});
```

**Example Problem:**
```
File: project.xer
- Task 100: Has predecessor Task 99
- Task 99: Has predecessor Task 98
- Task 98: Has predecessor Task 97

If we chunk TASK table:
- Chunk 1: Tasks 1-50
- Chunk 2: Tasks 51-100

When processing Predecessor for Task 100 → Task 99:
- Task 100 data is in Chunk 2
- Task 99 data is in Chunk 2
- BUT when processing predecessor Task 99 → Task 98:
  - Task 99 is in Chunk 2
  - Task 98 is in Chunk 1 (NOT IN MEMORY!)
  - LOOKUP FAILS! ❌
```

---

## Practical Hybrid Strategy (Best Approach)

### Strategy: Selective Chunking Based on Table Size

```csharp
public class PerformanceConfig
{
    // Only chunk tables larger than this
    public static int ChunkingThresholdRows { get; set; } = 100000;
    public static int ChunkSize { get; set; } = 50000;
}

public XerTable CreateEnhancedTableSmart(string tableName)
{
    var sourceTable = GetSourceTable(tableName);

    // Decision: Should we chunk this transformation?
    bool shouldChunk = sourceTable.RowCount > PerformanceConfig.ChunkingThresholdRows
                       && !RequiresFullTableLookup(tableName);

    if (shouldChunk)
    {
        return CreateTableStreamingVersion(tableName);
    }
    else
    {
        return CreateTableStandardVersion(tableName); // Your existing code
    }
}

private bool RequiresFullTableLookup(string tableName)
{
    // Tables that CANNOT be chunked because they're used as lookups
    return tableName == EnhancedTableNames.XerPredecessor06; // Needs full TASK lookup
}
```

### What to Chunk (Priority Order)

1. **✅ CSV Export** (Easiest, safest)
   - Chunk size: 10,000-50,000 rows
   - Memory reduction: 50-70%
   - Risk: None

2. **✅ Task Transformation (01_XER_TASK)**
   - Chunk size: 50,000 rows
   - Memory reduction: 80-90%
   - Risk: Low (lookups are small tables)

3. **✅ Simple Keyed Tables (02, 03, 07-14)**
   - Chunk size: 50,000 rows
   - Memory reduction: 80-90%
   - Risk: None (no complex lookups)

4. **⚠️ Calendar Detailed (11_XER_CALENDAR_DETAILED)**
   - Can chunk PARSING of clndr_data
   - CANNOT chunk the output (needed by Predecessor)
   - Memory reduction: 30-40%

5. **❌ Predecessor Transformation (06_XER_PREDECESSOR)**
   - CANNOT chunk without redesign
   - Requires full TASK table in memory
   - This is the real bottleneck for very large datasets

---

## Alternative Solutions for Predecessor Problem

### Option 1: Two-Pass Approach (Recommended)

**Strategy:** Extract minimal task data for lookups

```csharp
// Pass 1: Build lightweight task lookup (only fields needed for predecessor calc)
private Dictionary<string, MinimalTaskData> BuildLightweightTaskLookup(XerTable taskTable)
{
    var lookup = new Dictionary<string, MinimalTaskData>();

    // Only extract fields needed for predecessor calculation
    foreach (var rowData in taskTable.Rows)
    {
        var minimalData = new MinimalTaskData
        {
            ClndrIdKey = CreateKey(rowData.SourceFilename, GetField(row, "clndr_id")),
            StatusCode = GetField(row, "status_code"),
            TaskType = GetField(row, "task_type"),
            EarlyStartDate = ParseDate(GetField(row, "early_start_date")),
            EarlyEndDate = ParseDate(GetField(row, "early_end_date")),
            ActualStartDate = ParseDate(GetField(row, "act_start_date"))
        };

        string key = CreateKey(rowData.SourceFilename, GetField(row, "task_id"));
        lookup[key] = minimalData;
    }

    return lookup;
}

// Struct with only essential fields (much smaller memory footprint)
private struct MinimalTaskData
{
    public string ClndrIdKey;      // 8 bytes (reference)
    public string StatusCode;      // 8 bytes (interned)
    public string TaskType;        // 8 bytes (interned)
    public DateTime? EarlyStartDate;   // 9 bytes (nullable struct)
    public DateTime? EarlyEndDate;     // 9 bytes
    public DateTime? ActualStartDate;  // 9 bytes
    // TOTAL: ~50 bytes vs ~500+ bytes for full row
}

// Pass 2: Process TASKPRED in chunks using minimal lookup
public XerTable Create06XerPredecessor()
{
    var taskTable = _dataStore.GetTable(TableNames.Task);
    var taskPredTable = _dataStore.GetTable(TableNames.TaskPred);

    // Build minimal lookup (10x smaller than full task data)
    var minimalTaskLookup = BuildLightweightTaskLookup(taskTable);

    // Now we can process TASKPRED in chunks because lookup is much smaller
    int chunkSize = 50000;
    for (int offset = 0; offset < taskPredTable.RowCount; offset += chunkSize)
    {
        var chunk = taskPredTable.Rows.Skip(offset).Take(chunkSize);
        // Process chunk...
    }
}
```

**Memory Impact:**
```
Before:
- Full TASK lookup: 500K rows × 500 bytes = 250 MB

After:
- Minimal TASK lookup: 500K rows × 50 bytes = 25 MB (90% reduction!)
```

---

### Option 2: Database-Backed Lookups (For Extreme Scale)

For datasets >5GB, use SQLite for lookups:

```csharp
public class DatabaseBackedTaskLookup
{
    private SQLiteConnection _connection;

    public void BuildFromTable(XerTable taskTable)
    {
        // Create in-memory SQLite database
        _connection = new SQLiteConnection("Data Source=:memory:");

        // Bulk insert task data with indexes on composite keys
        using (var transaction = _connection.BeginTransaction())
        {
            foreach (var row in taskTable.Rows)
            {
                // Insert minimal task data
            }
            transaction.Commit();
        }

        // Create index on composite key
        ExecuteNonQuery("CREATE INDEX idx_task_key ON tasks(file_name, task_id)");
    }

    public MinimalTaskData Lookup(string compositeKey)
    {
        // Query by index (very fast)
        return ExecuteQuery($"SELECT * FROM tasks WHERE composite_key = '{compositeKey}'");
    }
}
```

**Benefits:**
- SQLite handles memory management
- Indexes make lookups fast (O(log n) vs O(1), but acceptable)
- Can spill to disk if needed
- Memory usage controlled by SQLite page cache

**Drawbacks:**
- Adds dependency on System.Data.SQLite
- Slower than in-memory dictionary (but acceptable for very large datasets)
- More complex code

---

## Recommended Implementation Plan

### Phase 1: Low-Hanging Fruit (1-2 weeks)

1. **Chunk CSV Export**
   - Modify `CsvExporter.WriteTableToCsv`
   - Add `GetRowsInChunks()` to `XerTable`
   - Expected: 50-70% memory reduction during export
   - Risk: None

2. **Chunk Simple Transformations**
   - Tables: 02, 03, 07, 08, 09, 10, 12, 13, 14
   - These have no complex lookups
   - Expected: 80% memory reduction
   - Risk: None

3. **Chunk Task Transformation (01_XER_TASK)**
   - Keep CALENDAR and PROJECT lookups in memory (tiny)
   - Chunk TASK table processing
   - Expected: 85% memory reduction
   - Risk: Low

**Total Expected Improvement:**
- Processing 100MB file: 1GB → 300MB (70% reduction)
- Processing 500MB file: 5GB → 1.2GB (76% reduction)

---

### Phase 2: Tackle Predecessor Problem (2-3 weeks)

1. **Implement Minimal Task Lookup**
   - Create `MinimalTaskData` struct
   - Build lightweight lookup (10x smaller)
   - Chunk TASKPRED processing

2. **Optional: Add SQLite Backend for Extreme Scale**
   - For files >2GB
   - Database-backed lookups
   - Configurable via `PerformanceConfig`

**Total Expected Improvement:**
- Processing 500MB file: 1.2GB → 500MB (90% reduction from original)
- Processing 2GB file: 8GB → 1.5GB (enables processing on 8GB RAM machines)

---

## Summary: What You Should Do

### ✅ Safe to Chunk (Do This First)
1. CSV Export - No dependencies
2. Simple keyed tables (02, 03, 07-14) - No complex lookups
3. Task transformation (01) - Small lookup tables

### ⚠️ Complex to Chunk (Do With Care)
4. Predecessor transformation (06) - Use minimal lookup strategy

### ❌ Don't Chunk
- CALENDAR table (tiny, used as lookup)
- PROJECT table (tiny, used as lookup)
- CALENDAR_DETAILED table (small-medium, used for working day calc)

### Key Principle
**Chunk SOURCE tables, keep LOOKUP tables in memory**

Your composite key pattern (`CreateKey(filename, id)`) is exactly right for this! It allows chunking source tables while maintaining cross-file relationships through in-memory lookups.

---

## Code Examples Ready to Use

I can provide complete implementations for:
1. Chunked CSV exporter (Program.cs:612-721)
2. Chunked Task transformation (Program.cs:823-983)
3. Minimal task lookup for Predecessor (Program.cs:1607-1638)
4. Smart chunking decision logic

Would you like me to implement any of these?
