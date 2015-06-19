#C# Client for Redis Cluster#

A dynamic link library developed in C#, to facilitate building client-side programs connecting to a Redis cluster. Generally speaking, it can be considered as a complement to `ServiceStack.Redis`.

As far as we know, this is the first C# client for Redis cluster. And it does not support transactions at present. But never mind, we are hard working to do our best to make it perfect.

##How to Build##

1. Open `build.bat`, set `CSHARP_HOME` to the directory where a `csc.exe` program can be found.
2. Run `build.bat`.

If everything goes well, 2 newly generated files can be found in the `bin` directory:

- `bin\ServiceStack.Cluster.dll`: The dynamic link library for Redis cluster client.
- `bin\Test.exe`: As its name implies, this is a test program for the purpose of demonstrating how to use the dynamic link library. By default, this program tries to connect to `127.0.0.1:6379`, and you can change this value in the `Test.cs` source file.

##How to Use##

1. Use namespace `ServiceStack.Cluster`.
2. Create an instance for class `RedisClusterClient`. The class constructor takes 2 parameters: an IPv4 string and a port number, pointing to one of the Redis server within the target Redis cluster.

Enjoy!
