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

        public MCRunTimeParameters()
        {
            Console.WriteLine();
            Console.WriteLine("(MC3)^3 v. 0.001");
            Console.WriteLine();

            iterations = Convert.ToInt32(getVal("Please enter the number of iterations: ", "100"));
            modelExecutable = getFile("Please enter the name of the executable file  : ", "persist_cmd.exe");
            modelParFile = getFile("Please enter the name of the .par file        : ", "test.par");
            modelDatFile = getFile("Please enter the name of the .dat file        : ", "test.dat");
            modelObsFile = getFile("Please enter the name of the .obs file        : ", "test.obs");
            minimumParFile = getFile("Please enter the name of the minimum .par file: ", "min.par");
            maximumParFile = getFile("Please enter the name of the maximum .par file: ", "max.par");
            Console.WriteLine();

            commandString = modelExecutable + " -par " + modelParFile + " -dat " + modelDatFile + " -obs " + modelObsFile + " -size small";

            modelResultsFile = getVal("Please enter the name of the file with performance statistics : ", "PERSIST_errors.csv");
            testStatisticColumnName = getVal("Please enter the test statistic (R2, NS, RMSE or RE)          : ", "NS");
            randomStartPoint = getVal("Do you wish to use a random start point (Y/N)                 : ", "N");
        }

        public MCRunTimeParameters(string[] args)
        {
            string argName;
            // each parameter needs a name and a value so the count should
            //equal half the number of command line arguments
            for (int i = 0; i < args.Length; i++)
            {
                argName = args[i].ToLower();

                switch (argName)
                {
                    case "-iterations":
                        this.iterations = Convert.ToInt32(args[++i]);
                        break;
                    case "-executable":
                        this.modelExecutable = args[++i];
                        break;
                    case "-par":
                        this.modelParFile = args[++i];
                        break;
                    case "-dat":
                        this.modelDatFile = args[++i];
                        break;
                    case "-obs":
                        this.modelObsFile = args[++i];
                        break;
                    case "-stats":
                        this.modelResultsFile = args[++i];
                        break;
                    case "-teststat":
                        this.testStatisticColumnName = args[++i];
                        break;
                }
            }
        }

        public int testStatisticColumn()
        {
            switch (this.testStatisticColumnName)
            {
                case "R2": return 1;
                case "NS": return 2;
                case "RMSE": return 3;
                case "RE": return 4;
                default:
                    Console.WriteLine("Inappropriate test statistic, please try R2, NS, RMSE or RE");
                    return -1;

            }
        }

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

        string getFile(string msg, string defaultValue)
        {
            string fileName = "";
            do
            {
                Console.Write(msg);
                fileName = Console.ReadLine();
                if (fileName.Length == 0)
                    fileName = defaultValue;
                if (File.Exists(fileName))
                    Console.WriteLine("Response: {0}", fileName);
                else
                {
                    Console.WriteLine("\n{0} does not appear to exist in the current directory", fileName);
                }
            }
            while (!File.Exists(fileName));
            return fileName;
        }
    }
}
