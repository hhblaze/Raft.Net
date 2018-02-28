- Change version/Release notes in .nuspec file
- Run !!!Build.bat




To create new framework configuration:

To create new framework configuration it is enough 
to set up new section in project file (like in Biser.csproj file for -NET47),
and add conditional compilation #if symbols into "DefineConstants" section

 <PropertyGroup Condition="'$(Configuration)|$(Platform)' == 'Release-NET47|AnyCPU'">
    <OutputPath>bin\Release-NET47\</OutputPath>
    <TargetFrameworkVersion>v4.7</TargetFrameworkVersion>
    <DefineConstants>TRACE</DefineConstants>
    <AllowUnsafeBlocks>false</AllowUnsafeBlocks>
    <DocumentationFile>bin\Release-NET47\Biser.xml</DocumentationFile>
    <Optimize>true</Optimize>
    <DebugType>pdbonly</DebugType>
    <PlatformTarget>AnyCPU</PlatformTarget>
    <ErrorReport>prompt</ErrorReport>
    <CodeAnalysisRuleSet>MinimumRecommendedRules.ruleset</CodeAnalysisRuleSet>
  </PropertyGroup>
  
  then setup msbuild to compile this sectio separately in !!!Build.bat like for .NET47:
  "C:\Program Files (x86)\Microsoft Visual Studio\2017\Professional\MSBuild\15.0\Bin\msbuild.exe" "%~dp0..\Biser\Biser.csproj" /t:rebuild /p:Configuration=Release-NET47
  
  add new framework items into .nuspec file