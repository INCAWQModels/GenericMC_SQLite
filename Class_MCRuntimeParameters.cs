using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace MC
{
    public class MCRunTimeParameters
    {
        public long iterations;
        public string commandString;
        string modelExecutable;
        public string modelParFile;
        public string minimumParFile;
        public string maximumParFile;
        string modelDatFile;
        string modelObsFile;
        public string modelResultsFile;
        public string arrayOutputFile = "out.csv";
        string testStatisticColumnName;
        public string randomStartPoint;

        public int maximumUnsuccessfulJumps = 5;

        string getVal(string msg, string defaultValue)
        {
            string response;
            Console.Write(msg);

            response = Console.ReadLine();
            if (response.Length == 0)
            {
                response = defaultValue;
            }
            response = response.ToUpper();
            Console.WriteLine("Response: {0}", response);
            return response;
        }
    }
}
