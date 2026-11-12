```

BenchmarkDotNet v0.13.12, Windows 11 (10.0.26200.9974)
Unknown processor
.NET SDK 8.0.428
  [Host]   : .NET 8.0.34 (8.0.3426.52217), X64 RyuJIT AVX2
  ShortRun : .NET 8.0.34 (8.0.3426.52217), X64 RyuJIT AVX2

Job=ShortRun  IterationCount=3  LaunchCount=1  
WarmupCount=3  

```
| Method          | Mean       | Error     | StdDev   | Gen0   | Gen1   | Allocated |
|---------------- |-----------:|----------:|---------:|-------:|-------:|----------:|
| EncodeHeartbeat |   154.8 ns |  21.56 ns |  1.18 ns | 0.0153 |      - |      96 B |
| EncodeAppend    | 1,524.7 ns | 191.53 ns | 10.50 ns | 0.0191 |      - |     128 B |
| DecodeAppend    | 1,038.8 ns |  54.57 ns |  2.99 ns | 1.7796 | 0.0687 |   11168 B |
