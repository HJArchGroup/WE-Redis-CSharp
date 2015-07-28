using System;
using System.Collections.Generic;
using ServiceStack.Redis;

namespace ServiceStack.Cluster
{
    public class ConnectionPool
    {
        private Dictionary<ulong, RedisNativeClient> clients;
        private Dictionary<string, ClusterNode> nodes;
        private RedisNativeClient[] slots;

        private HashSet<RedisNativeClient> disconnected;

        private string password;

        public ConnectionPool(string ip, ushort port, string password)
        {
            this.clients = new Dictionary<ulong, RedisNativeClient>();
            this.nodes = new Dictionary<string, ClusterNode>();
            this.slots = new RedisNativeClient[Global.HashSlotSize];

            this.disconnected = new HashSet<RedisNativeClient>();

            if ((password != null) && (password.Length > 0))
            {
                this.password = password;
            }
            else
            {
                this.password = null;
            }

            this.Update(this.GetRedisClient(ip, port), ip);
        }

        public void Update()
        {
            // Get a connected Redis client.
            ClusterNode node = null;
            RedisNativeClient client = null;
            foreach (var item in this.nodes)
            {
                node = item.Value;
                if (node.IsConnected())
                {
                    client = this.GetRedisClient(node.ip, node.port);
                    if (this.disconnected.Contains(client))
                    {
                        // TODO: The quit operation seems unnecessary.
                        client.Quit();
                        this.disconnected.Remove(client);
                        this.clients.Remove(Network.IPv4ToUlong(node.ip, node.port));
                        
                        // This Redis client has to reconnect to the Redis cluster.
                        client = this.GetRedisClient(node.ip, node.port);
                    }

                    // Return a connected Redis client.
                    break;
                }
            }

            if (client == null)
            {
                // TODO: All clients are disconnected.
                return;
            }

            // Update information on this Redis cluster.
            this.Update(client, node.ip);
        }
        private void Update(RedisNativeClient client, string ip)
        {
            if (client == null)
            {
                Console.WriteLine("client == null");
                return;
            }
            
            RedisData reply = null;
            try
            {
                reply = client.RawCommand("CLUSTER", "NODES");
            }
            catch (RedisException e)
            {
                if (e.Message.Contains("cluster support disabled"))
                {
                    Global.Info(ip, "Switch to non-cluster mode");
                    for (int i = 0; i < Global.HashSlotSize; i++)
                    {
                        this.slots[i] = client;
                    }
                }
                else
                {
                    Global.Error(ip, e.Message);
                }
                return;
            }
            
            if (reply != null)
            {
                this.ParseClusterNodes(reply.ToRedisText().Text, ip);
            }
        }
        private void ParseClusterNodes(string s, string originalIP)
        {
            if (s == null)
            {
                return;
            }

            int len = s.Length - 1;
            int i = 0;
            int start = i;

            ClusterNode node = null;
            RedisNativeClient client = null;

            while (i < len)
            {
                // UUID.
                start = i;
                while (s[i] != ' ') i++;
                node = this.GetClusterNode(s.Substring(start, i - start));

                // IP.
                i++;
                start = i;
                while (s[i] != ':') i++;
                node.ip = s.Substring(start, i - start);

                // Port.
                i++;
                start = i;
                while (s[i] != ' ') i++;
                node.port = ushort.Parse(s.Substring(start, i - start));

                // Record this client.
                if (originalIP != null)
                {
                    if (Global.LocalhostIPv4.CompareTo(node.ip) == 0)
                    {
                        node.ip = originalIP;
                    }
                }
                client = this.GetRedisClient(node.ip, node.port);

                // Status.
                i++;
                start = i;
                while (s[i] != ' ') i++;
                if (s.Substring(start, i - start).Contains("master"))
                {
                    // This is a master.
                    node.SetMaster();

                    i += 3;
                }
                else
                {
                    // This is a slave.
                    node.SetSlave();

                    // Master UUID.
                    i++;
                    start = i;
                    while (s[i] != ' ') i++;
                    node.masterUUID = s.Substring(start, i - start);

                    i++;
                }

                // Number of sent PING commands.
                start = i;
                while (s[i] != ' ') i++;
                node.ping = ulong.Parse(s.Substring(start, i - start));

                // Number of received PONG replies.
                i++;
                start = i;
                while (s[i] != ' ') i++;
                node.pong = ulong.Parse(s.Substring(start, i - start));

                // Epoch.
                i++;
                start = i;
                while (s[i] != ' ') i++;
                node.epoch = ulong.Parse(s.Substring(start, i - start));

                // "connected" or "disconnected".
                i++;
                start = i;
                while ((i < len) && (s[i] != ' ') && (s[i] != '\r') && (s[i] != '\n')) i++;
                if ("connected".CompareTo(s.Substring(start, i - start)) == 0)
                {
                    node.SetConnected();

                    if (node.IsMaster())
                    {
                        if ((i < len) && (s[i] != '\r') && (s[i] != '\n'))
                        {
                            // Start slot.
                            i++;
                            start = i;
                            while (s[i] != '-') i++;
                            node.startSlot = ushort.Parse(s.Substring(start, i - start));

                            // End slot.
                            i++;
                            start = i;
                            while (s[i] != '\r' && s[i] != '\n') i++;
                            node.endSlot = ushort.Parse(s.Substring(start, i - start));

                            this.SetHashSlots(node.ip, node.port, node.startSlot, node.endSlot);
                        }
                    }
                }
                else
                {
                    node.SetDisconnected();
                }

                while (i < len)
                {
                    if (s[i] == '\n')
                    {
                        break;
                    }
                    i++;
                }
            }
        }

        public void Close()
        {
            foreach (var item in this.nodes)
            {
                ClusterNode node = item.Value;
                if (node.IsConnected())
                {
                    RedisNativeClient client = null;
                    if (this.clients.TryGetValue(Network.IPv4ToUlong(node.ip, node.port), out client))
                    {
                        client.Quit();
                    }
                }
                node.Clear();
            }
            this.nodes.Clear();
            this.clients.Clear();

            this.disconnected.Clear();

            for (var i = 0; i < Global.HashSlotSize; ++i)
            {
                this.slots[i] = null;
            }
        }

        // Operations for Hash slots.
        public int SetHashSlots(string ip, ushort port, int start, int end)
        {
            RedisNativeClient client = this.GetRedisClient(ip, port);
            if (client == null)
            {
                return 0;
            }

            for (int i = start; i <= end; ++i)
            {
                try
                {
                    this.slots[i] = client;
                }
                catch (ArrayTypeMismatchException e)
                {
                    Console.WriteLine(e.ToString());
                }
            }
            return (end - start);
        }
        public RedisNativeClient[] GetSlots()
        {
            return this.slots;
        }

        // Operations for Redis cluster nodes.
        public ClusterNode GetClusterNode(string uuid)
        {
            ClusterNode node = null;
            if (!this.nodes.TryGetValue(uuid, out node))
            {
                node = new ClusterNode(uuid);
                this.nodes.Add(uuid, node);
            }
            return node;
        }

        // Operations for Redis clients.
        public RedisNativeClient GetRedisClient(string ip, ushort port)
        {
            ulong key = Network.IPv4ToUlong(ip, port);

            RedisNativeClient client = null;
            if (!this.clients.TryGetValue(key, out client))
            {
                client = new RedisNativeClient(ip, port);
                this.clients.Add(key, client);

                if ((this.password != null) && (this.password.Length > 0))
                {
                    client.RawCommand("AUTH", this.password);
                }
            }
            return client;
        }
        private bool IsValidKey(string key)
        {
            if (key == null || key.Length <= 0)
            {
                return false;
            }

            return true;
        }
        public RedisNativeClient GetRedisClient(string key)
        {
            if (!this.IsValidKey(key))
            {
                return null;
            }

            int id = CRC16.Compute(key) % Global.HashSlotSize;
#if DEBUG
            RedisNativeClient client = this.slots[id];
            if (client == null)
            {
                ConsoleColor color = Console.ForegroundColor;
                Console.Write("[");

                Console.ForegroundColor = Global.ColorError;
                Console.Write("ERROR");

                Console.ForegroundColor = color;
                Console.Write("] Cannot find a corresponding client for key ");

                Console.ForegroundColor = Global.ColorEmphasis;
                Console.Write(key);

                Console.ForegroundColor = color;
                Console.Write(", and its Hash slot ID is ");

                Console.ForegroundColor = Global.ColorEmphasis;
                Console.Write(id);

                Console.ForegroundColor = color;
                Console.WriteLine(".");
            }
            return client;
#else
            return this.slots[id];
#endif
        }
        public RedisNativeClient GetRedisClient(params string[] keys)
        {
            RedisNativeClient client = null;
            RedisNativeClient tmp = null;

            for (var i = 0; i < keys.Length; i++)
            {
                // Get the current client.
                tmp = this.GetRedisClient(keys[i]);
                if (tmp == null)
                {
                    return null;
                }

                // Check whether all clients are of the same.
                if (client == null)
                {
                    client = tmp;
                }
                else
                {
                    if (client != tmp)
                    {
#if DEBUG
                        ConsoleColor color = Console.ForegroundColor;
                        Console.Write("[");

                        Console.ForegroundColor = Global.ColorError;
                        Console.Write("ERROR");

                        Console.ForegroundColor = color;
                        Console.Write("] Cannot find a single client for keys ");

                        Console.ForegroundColor = Global.ColorEmphasis;
                        Console.Write(keys[i - 1]);

                        Console.ForegroundColor = color;
                        Console.Write(" and ");

                        Console.ForegroundColor = Global.ColorEmphasis;
                        Console.Write(keys[i]);

                        Console.ForegroundColor = color;
                        Console.WriteLine(".");
#endif
                        return null;
                    }
                }
            }

            return client;
        }
        public Dictionary<ulong, RedisNativeClient> GetClients()
        {
            return this.clients;
        }

        public void FlushAll()
        {
            foreach (var item in this.clients)
            {
                RedisNativeClient client = item.Value;
                client.FlushAll();
            }
        }
        
        override
        public string ToString()
        {
            string result = "";
            foreach (var item in this.nodes)
            {
                result += item.Value.ToString() + "\r\n";
            }
            return result;
        }
    }
}
