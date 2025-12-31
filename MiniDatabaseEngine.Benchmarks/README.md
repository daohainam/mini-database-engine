# MiniDatabaseEngine Benchmarks

This project contains performance benchmarks for the Mini Database Engine using [BenchmarkDotNet](https://benchmarkdotnet.org/).

## Benchmarks

The benchmark suite includes tests for common database operations:

### Read Operations
- **ReadSmallRecords**: Benchmark for reading records with 3 columns (Id, Name, Value)
- **ReadLargeRecords**: Benchmark for reading records with 15 columns (including strings, dates, and numeric fields)

### Write/Insert Operations
- **InsertSmallRecords**: Benchmark for inserting small records
- **InsertLargeRecords**: Benchmark for inserting large records

### Update Operations
- **UpdateSmallRecords**: Benchmark for updating small records
- **UpdateLargeRecords**: Benchmark for updating large records

### Delete Operations
- **DeleteAndReInsertSmallRecords**: Benchmark for deleting and re-inserting small records
- **DeleteAndReInsertLargeRecords**: Benchmark for deleting and re-inserting large records

## Running Benchmarks

### Run All Benchmarks
```bash
cd MiniDatabaseEngine.Benchmarks
dotnet run -c Release
```

### Run Specific Benchmark
```bash
dotnet run -c Release -- --filter "*ReadSmallRecords"
```

### Run Benchmarks by Category
```bash
# Run all read benchmarks
dotnet run -c Release -- --filter "*Read*"

# Run all insert benchmarks
dotnet run -c Release -- --filter "*Insert*"

# Run all update benchmarks
dotnet run -c Release -- --filter "*Update*"

# Run all delete benchmarks
dotnet run -c Release -- --filter "*Delete*"
```

### List Available Benchmarks
```bash
dotnet run -c Release -- --list flat
```

### Quick Test (Dry Run)
For a quick test without full benchmark execution:
```bash
dotnet run -c Release -- --job dry
```

## Results

Benchmark results are saved in the `BenchmarkDotNet.Artifacts/results` directory and include:
- CSV file for data analysis
- HTML report for visual representation
- Markdown file for documentation

## Benchmark Configuration

- **Memory Diagnostics**: Enabled to track memory allocations
- **Database Cache Size**: 500 pages
- **Pre-populated Records**: 1000 records per table
- **Operations per Benchmark**: 100 operations

## Understanding Results

The benchmark output includes:
- **Mean**: Average execution time
- **Error**: Half of 99.9% confidence interval
- **StdDev**: Standard deviation of measurements
- **Allocated**: Memory allocated per operation
- **Gen0/Gen1/Gen2**: Garbage collection statistics

## Requirements

- .NET 10.0 or later
- Release build configuration (recommended for accurate results)

## Notes

- Benchmarks use temporary database files that are automatically cleaned up
- Each benchmark includes setup and cleanup phases
- Results may vary based on hardware and system load
- For accurate results, close other applications and run in Release mode
