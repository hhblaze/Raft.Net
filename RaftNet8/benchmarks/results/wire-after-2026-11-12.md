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
| EncodeHeartbeat |   115.1 ns |   3.91 ns |  0.21 ns | 0.0050 |      - |      32 B |
| EncodeAppend    | 1,631.6 ns |  46.61 ns |  2.56 ns | 0.0038 |      - |      32 B |
| DecodeAppend    | 1,052.0 ns | 329.29 ns | 18.05 ns | 1.7796 | 0.0687 |   11168 B |
