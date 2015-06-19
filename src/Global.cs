using System;

namespace ServiceStack.Cluster
{
    public class Global
    {
        public const ConsoleColor ColorError = ConsoleColor.Red;
        public const ConsoleColor ColorWarning = ConsoleColor.Magenta;
        public const ConsoleColor ColorInfo = ConsoleColor.Green;
        public const ConsoleColor ColorEmphasis = ConsoleColor.Yellow;

        public const int MaxValueLength = 1073741824;
        public const ushort HashSlotSize = 16384;
        
        public const string LocalhostIPv4 = "127.0.0.1";
        
        public const string TestIP = "192.168.177.61";
        public const ushort TestPort = 10011;

        private const int ErrorType     = 1;
        private const int WarningType   = 2;
        private const int InfoType      = 3;
        private static void PrintMessage(int type, string emphasis, string message)
        {
            ConsoleColor color = Console.ForegroundColor;
            Console.Write("[");

            switch (type)
            {
                case Global.ErrorType:
                    Console.ForegroundColor = Global.ColorError;
                    Console.Write("ERROR");
                    break;

                case Global.WarningType:
                    Console.ForegroundColor = Global.ColorWarning;
                    Console.Write("WARNING");
                    break;

                case Global.InfoType:
                    Console.ForegroundColor = Global.ColorInfo;
                    Console.Write("INFO");
                    break;

                default:
                    break;
            }

            if ((emphasis != null) && (!emphasis.IsEmpty()))
            {
                Console.ForegroundColor = Global.ColorEmphasis;
                Console.Write(" " + emphasis);
            }

            Console.ForegroundColor = color;
            Console.Write("]");

            if ((message != null) && (!message.IsEmpty()))
            {
                Console.WriteLine(" " + message + ".");
            }
        }
        public static void Error(string emphasis, string message)
        {
            Global.PrintMessage(Global.ErrorType, emphasis, message);
        }
        public static void Warning(string emphasis, string message)
        {
            Global.PrintMessage(Global.WarningType, emphasis, message);
        }
        public static void Info(string emphasis, string message)
        {
            Global.PrintMessage(Global.InfoType, emphasis, message);
        }
    }
}
