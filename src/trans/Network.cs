namespace ServiceStack.Cluster
{
    public class Network
    {
        public static ulong IPv4ToUlong(string ip, ushort port)
        {
            if (ip == null || ip.Length == 0 || port == 0)
            {
                return 0x0000000000000000;
            }

            ulong result = 0x0000000000000000;
            ulong n = 0;
            int dotCount = 0;

            for (int i = 0; i < ip.Length; ++i)
            {
                switch (ip[i])
                {
                    case '0':
                    case '1':
                    case '2':
                    case '3':
                    case '4':
                    case '5':
                    case '6':
                    case '7':
                    case '8':
                    case '9':
                        n *= 10;
                        n += (ulong)(ip[i] - '0');

                        break;

                    case '.':
                        dotCount++;
                        if (dotCount > 3)
                        {
                            return 0;
                        }

                        result *= 256;
                        result += n;

                        n = 0;

                        break;

                    default:
                        return 0;
                }
            }

            if (dotCount != 3)
            {
                return 0;
            }

            result *= 256;
            result += n;

            // Append the port.
            result *= 65536;
            result += port;

            return result;
        }
    }
}
