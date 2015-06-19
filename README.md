#C# Client for Redis Cluster#

A client library for Redis cluster in CSharp built on `ServiceStack.Redis`.

As far as we know, this is the first C# client for Redis cluster. And it does not support transactions at present. But never mind, we are hard working to do our best to make it perfect.

##How to Build##

1. Open `build.bat`, set `CSHARP_HOME` to the directory where a `csc.exe` can be found.
2. Run `build.bat`.

If everything goes well, a dynamic link library (i.e. `bin\ServiceStack.Cluster.dll`) and an exectutable program (i.e. `bin\Test.exe`) will be generated.
The program tries to connected `127.0.0.1:6379` by default, and you can change it in the `Test.cs` source file.

##How to Use##

1. Use namespace 'ServiceStack.Cluster'.
2. Create an instance for class 'RedisClusterClient'.

Enjoys!


