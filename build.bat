@ECHO OFF
CLS

SET CSHARP_HOME=C:\Windows\Microsoft.NET\Framework64\v4.0.30319

SET RES=bin/ServiceStack.Common.dll;bin/ServiceStack.Interfaces.dll;bin/ServiceStack.Redis.dll;bin/ServiceStack.Text.dll

%CSHARP_HOME%\csc.exe /target:library /out:bin/ServiceStack.Cluster.dll /r:%RES% /recurse:src\*.cs
%CSHARP_HOME%\csc.exe /target:exe /out:bin/Test.exe /r:%RES%;bin/ServiceStack.Cluster.dll Test.cs

PAUSE
