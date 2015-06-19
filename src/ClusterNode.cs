using System;
using System.Collections.Generic;

namespace ServiceStack.Cluster
{
    public class ClusterNode
    {
        public string ip;
        public ushort port;

        public ulong epoch;
        public ulong ping;
        public ulong pong;

        public ushort startSlot;
        public ushort endSlot;

        public string masterUUID;

        public ClusterNode(string UUID)
        {
            this.UUID = UUID;
            this.Clear();
        }

        private string UUID;
        public string GetUUID()
        {
            return this.UUID;
        }

        public void Clear()
        {
            this.connected = false;
            this.master = false;
            this.masterUUID = null;

            this.epoch = 0;
            this.ping = 0;
            this.ping = 0;

            this.startSlot = 0;
            this.endSlot = 0;

            if (this.slaves != null)
            {
                this.slaves.Clear();
            }
        }

        private bool master;
        public void SetMaster()
        {
            this.master = true;
            this.masterUUID = null;
            if (this.slaves != null)
            {
                this.slaves.Clear();
            }
        }
        public void SetSlave()
        {
            this.master = false;
        }
        public bool IsMaster()
        {
            return this.master;
        }

        private bool connected;
        public void SetConnected()
        {
            this.connected = true;
        }
        public void SetDisconnected()
        {
            this.connected = false;
        }
        public bool IsConnected()
        {
            return this.connected;
        }

        private HashSet<ClusterNode> slaves;
        public void AddSlave(ClusterNode node)
        {
            if (this.slaves == null)
            {
                this.slaves = new HashSet<ClusterNode>();
            }
            this.slaves.Add(node);
            this.master = true;
        }
        public HashSet<ClusterNode> GetSlaves()
        {
            return this.slaves;
        }
        
        override
        public string ToString()
        {
            string result = this.UUID + " " + this.ip + ":" + this.port + "(" + this.epoch + ")";
            
            if (this.master)
            {
                result += " Master " + this.startSlot + "-" + this.endSlot;
            }
            else
            {
                result += " Slave " + this.masterUUID;
            }
            
            if (this.connected)
            {
                result += " Connected";
            }
            else
            {
                result += " Disconnected";
            }
            
            result += " " + this.ping + "|" + this.pong;
            
            return result;
        }
    }
}
