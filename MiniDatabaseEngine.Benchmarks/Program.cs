using BenchmarkDotNet.Running;
using MiniDatabaseEngine.Benchmarks;

BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run(args);
