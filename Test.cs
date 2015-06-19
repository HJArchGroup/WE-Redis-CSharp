using System;
using ServiceStack.Cluster;

namespace ServiceStack.Cluster
{
    public class Test
    {
        public static void Main()
        {
            int size = 10000;
            int cnt = 0;
            string key = null;
            string field = null;
            string value = null;
            string[] result = null;
            RedisClusterClient client = new RedisClusterClient("127.0.0.1", 6379);

            //------------------------------------------------------------------
            // Phase : HASH

            Console.WriteLine();
            Console.WriteLine("--------------------------------------------------");
            Global.Info(null, "HASH Phase begin ..");

            key = "HASHKEY";
            for (int i = 0; i < size; ++i)
            {
                field = "FIELD" + i;
                value = "VALUE" + i;
                client.HSet(key, field, value);
            }
            result = client.HKeys(key);
            Global.Info(null, result.GetLength(0) + " of " + size + " Hash fields have been set");
            Global.Info(null, "HASH Phase finished");

            //------------------------------------------------------------------
            // Phase : INCR & DECR

            Console.WriteLine();
            Console.WriteLine("--------------------------------------------------");
            Global.Info(null, "INCR & DECR Phase begin ..");

            key = "COUNTER";
            client.Set(key, "100");
            client.Incr(key);
            if ("101".CompareTo(client.Get(key)) != 0)
            {
                Global.Error(null, "failed to increase " + key);
            }
            Global.Info(null, "INCR & DECR Phase finished");

            //------------------------------------------------------------------
            // Phase : SET

            Console.WriteLine();
            Console.WriteLine("--------------------------------------------------");
            Global.Info(null, "SET Phase begin ..");
            cnt = 0;
            for (int i = 0; i < size; ++i)
            {
                key = "KEY" + i;
                if (!client.Set(key, "okay"))
                {
                    Global.Error(null, "failed to write " + key);
                }
                else
                {
                    cnt++;
                }
            }
            if (cnt == size)
            {
                Global.Info(null, cnt + " of " + size + " keys have been set");
            }
            else
            {
                Global.Warning(null, cnt + " of " + size + " keys have been set");
            }
            Global.Info(null, "SET Phase finished");

            //------------------------------------------------------------------
            // Phase : GET

            Console.WriteLine();
            Console.WriteLine("--------------------------------------------------");
            Global.Info(null, "GET Phase begin ..");
            cnt = 0;
            for (int i = 0; i < size; ++i)
            {
                key = "KEY" + i;
                if ("okay".CompareTo(client.Get(key)) != 0)
                {
                    Global.Error(null, "failed to got " + key);
                }
                else
                {
                    cnt++;
                }
            }
            if (cnt == size)
            {
                Global.Info(null, cnt + " of " + size + " keys have been gotten");
            }
            else
            {
                Global.Warning(null, cnt + " of " + size + " keys have been gotten");
            }
            Global.Info(null, "GET Phase finished");
        }
    }
}
