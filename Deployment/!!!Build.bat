"C:\Program Files (x86)\Microsoft Visual Studio\2017\Professional\MSBuild\15.0\Bin\msbuild.exe" "%~dp0..\Raft\Raft.csproj" /t:rebuild /p:Configuration=Release
"C:\Program Files (x86)\Microsoft Visual Studio\2017\Professional\MSBuild\15.0\Bin\msbuild.exe" "%~dp0..\Raft\Raft.csproj" /t:rebuild /p:Configuration=Release-NET47
"C:\Program Files (x86)\Microsoft Visual Studio\2017\Professional\MSBuild\15.0\Bin\msbuild.exe" "%~dp0..\RaftStandard\RaftStandard.csproj" /t:rebuild /p:Configuration=Release

nuget.exe pack "%~dp0!!!Raft.nuspec" -BasePath "%~dp0.." -OutputDirectory "%~dp0..\Deployment"