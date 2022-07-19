using System;
using System.IO;

namespace MC
{
    /// <summary>
    /// store and process GLUE results
    /// </summary>
    public static class GLUEAccounting
    {
        public static bool runningGLUE = false;
        public static bool saveForGLUE = false;
        static long runCounter = 0;

        /// <summary>
        /// save a parameter set as a file with a GLUE identifier
        /// </summary>
        /// <param name="p">parameter set to save</param>
        /// <returns></returns>
        public static int SaveGLUEResults(parameterSet p)
        {
            int returnVal;
            if (saveForGLUE)
            {
                string fileName = "GLUE_" + runCounter.ToString() + ".par";
                returnVal = p.write(fileName);
                using (StreamWriter w = File.AppendText("GLUEPerformance.csv"))
                {
                    w.WriteLine(runCounter.ToString() + ", "+p.modelPerformance.ToString());
                }
                runCounter++;
            }
            else returnVal = 0;
            saveForGLUE = false;


            return returnVal;
        }
        /// <summary>
        /// run an interation of a GLUE analysis
        /// </summary>
        /// <param name="commandLine">INCA command string to run</param>
        /// <param name="runNumber">run number in GLUE chain</param>
        public static void GLUERun(string commandLine, long runNumber)
        {
            double testModelPerformance;
            parameterSet g = new parameterSet(MCParameters.MCParFile, MCParameters.minParFile(), MCParameters.maxParFile());
            g.randomizeValues();
            g.write(MCParameters.MCParFile);
            InteractWithModel.RunModel(commandLine);
            string targetFileName = "GLUE_ParameterSet_" + runNumber.ToString() + ".par";
            File.Copy(MCParameters.MCParFile, targetFileName);
            testModelPerformance = InteractWithModel.EvaluatePerformanceStatistics();
            Console.WriteLine(runNumber.ToString() + ", " + testModelPerformance.ToString());
            using (StreamWriter w = File.AppendText("GLUEPerformance.csv"))
            {
                w.WriteLine(runNumber.ToString() + ", " + testModelPerformance.ToString());
            }
        }
    }
}

