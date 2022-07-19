using System;
using System.IO;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace MC
{
    class SummarizeResults
    {
        public static void write(string modelCommandLine)
        {
            int iterations = MCParameters.maxTries;

            //remove output files from older runs
            if(File.Exists(MCParameters.parameterArrayFileName))
                File.Delete(MCParameters.parameterArrayFileName);
            if(File.Exists(MCParameters.parameterNameListFile))
                File.Delete(MCParameters.parameterNameListFile);
            if (File.Exists(MCParameters.parameterValueListFile))
                File.Delete(MCParameters.parameterValueListFile);

            resultsDatabase rd = new resultsDatabase();
            rd.makeCoefficientsTable();
            rd.makeResultsTable();

            //switch doesn't seem to work so use a bunch of if statements, really not very elegant but ....
            //needs to be rethought as it is getting more than a little messy
            //
            if (MCParameters.model == 1) // running PERSiST 1.4
            {
                PERSiSTParameterArrayList parList = new PERSiSTParameterArrayList(MCParameters.numberOfLandUses, MCParameters.numberOfReaches, MCParameters.numberOfBoxes);
                parList.write(MCParameters.parameterNameListFile);
                rd.writeParameterNames(parList);
            }
            if (MCParameters.model == 2) // running INCA-C
            {
                INCA_CParameterArrayList parList = new INCA_CParameterArrayList(MCParameters.numberOfLandUses, MCParameters.numberOfReaches);
                parList.write(MCParameters.parameterNameListFile);
                rd.writeParameterNames(parList);
            }
            if(MCParameters.model==3)   // running INCA_PEco
            {
                INCA_PEcoParameterArrayList parList = new INCA_PEcoParameterArrayList(MCParameters.numberOfLandUses, MCParameters.numberOfReaches);
                parList.write(MCParameters.parameterNameListFile);
                rd.writeParameterNames(parList);
            }
            if (MCParameters.model == 4)
            {
                INCA_PParameterArrayList parList = new INCA_PParameterArrayList(MCParameters.numberOfLandUses, MCParameters.numberOfReaches);
                parList.write(MCParameters.parameterNameListFile);
                rd.writeParameterNames(parList);
            }
            if (MCParameters.model == 5)
            {
                INCA_ToxParameterArrayList parList = new INCA_ToxParameterArrayList(MCParameters.numberOfLandUses, MCParameters.numberOfReaches, MCParameters.numberOfSedimentClasses, MCParameters.numberOfContaminants);
                parList.write(MCParameters.parameterNameListFile);
                rd.writeParameterNames(parList);
            }
            if (MCParameters.model == 6)
            {
                INCA_PathParameterArrayList parList = new INCA_PathParameterArrayList(MCParameters.numberOfLandUses, MCParameters.numberOfReaches);
                parList.write(MCParameters.parameterNameListFile);
                rd.writeParameterNames(parList);
            }
            if(MCParameters.model==7)
            {
                INCA_HgParameterArrayList parList = new INCA_HgParameterArrayList(MCParameters.numberOfLandUses, MCParameters.numberOfReaches);
                parList.write(MCParameters.parameterNameListFile);
                rd.writeParameterNames(parList);
            }
            if (MCParameters.model == 8) // running PERSiST 1.6
            {
                PERSiST1_6ParameterArrayList parList = new PERSiST1_6ParameterArrayList(MCParameters.numberOfLandUses, MCParameters.numberOfReaches, MCParameters.numberOfBoxes);
                parList.write(MCParameters.parameterNameListFile);
                rd.writeParameterNames(parList);
            }
            if(MCParameters.model==9)   //running INCA(ON)THE
            {
                INCA_ONTHEParameterArrayList parList = new INCA_ONTHEParameterArrayList(MCParameters.numberOfLandUses, MCParameters.numberOfReaches);
                parList.write(MCParameters.parameterNameListFile);
                rd.writeParameterNames(parList);
            }
            if(MCParameters.model==10)  // running PERSiST v2.x
            {
                PERSiST_v2ParameterArrayList parList = new PERSiST_v2ParameterArrayList(MCParameters.numberOfLandUses, MCParameters.numberOfReaches, MCParameters.numberOfBoxes);
                parList.write(MCParameters.parameterNameListFile);
                rd.writeParameterNames(parList);
            }
            if(MCParameters.model==12) //running INCA-N classic
            {
                INCA_NParameterArrayList parList = new INCA_NParameterArrayList(MCParameters.numberOfLandUses, MCParameters.numberOfReaches);
                parList.write(MCParameters.parameterNameListFile);
                rd.writeParameterNames(parList);
            }
            //write the model outputs to file
            parameterSet p = new parameterSet(MCParameters.minParFile());
            p.writeToFileAsList(-2, MCParameters.parameterValueListFile);
            rd.writeParameterSet(-2, p);
            p = new parameterSet(MCParameters.maxParFile());
            p.writeToFileAsList(-1, MCParameters.parameterValueListFile);
            rd.writeParameterSet(-1, p);
            //clear out the coefficients file
            if (File.Exists(MCParameters.coefficientsSummaryFile))
            {
                File.Delete(MCParameters.coefficientsSummaryFile);
            }

            //clean up the results files
            deleteResults();
            if (MCParameters.model == 1 ^ MCParameters.model==8 ^ MCParameters.model==10)
                deleteINCAInputsFromPERSiST();

            for (int i = 0; i < MCParameters.runsToOrganize; i++)
            {
                runOnce(i, modelCommandLine, MCParameters.parameterArrayFileName);
            }

            //write the coefficients for each model run
            rd.writeCoefficients();

            //write the results (at a later date)
            //rd.writeResults();
        }

        static void getNumberOfRunsToOrganize()
        {
            string r;
            int s;

            Console.Write("Please enter the number of runs to organize : ");
            r = Console.ReadLine();
            try
            {
                s = Convert.ToInt32(r);
                MCParameters.runsToOrganize = s;
            }
            catch
            {
                Console.WriteLine("You did not enter a valid number, the number of runs has been set to {0}", MCParameters.runsToOrganize);
            }
        }

        static void runOnce(int runNumber, string modelCommandLine, string outputFileName)
        {
            parameterSet p = new parameterSet(MCParameters.MCParFile);

            MCParameters.bestParSetFileName = "bestParSet" + runNumber.ToString() + ".par";
            File.Copy(MCParameters.bestParSetFileName, MCParameters.MCParFile, true);
            p.writeToFileAsList(runNumber, MCParameters.parameterValueListFile);

            resultsDatabase rd = new resultsDatabase();
            rd.writeParameterSet(runNumber, p);

            InteractWithModel.RunModel(modelCommandLine);
            double testModelPerformance = InteractWithModel.EvaluatePerformanceStatistics();
            if(MCParameters.GLUE==2)
            {
                using (StreamWriter w = File.AppendText("GLUEComparison.csv"))
                {
                    w.WriteLine(runNumber.ToString() + ", " + testModelPerformance.ToString());
                }
            }

            //write the parameter set to a file containing all parameter sets
            p = new parameterSet(MCParameters.bestParSetFileName);
            p.writeToArray(runNumber.ToString(), outputFileName);

            //write the model performance coeficients to an an output file for later analysis
            string[] coefficients = File.ReadAllLines(MCParameters.coefficientsFile);

            int k = 0;

            using (StreamWriter sw1 = File.AppendText(MCParameters.coefficientsSummaryFile))
            {
                foreach (string coefficient in coefficients)
                {
                    string trackableCoefficient = runNumber.ToString() + MCParameters.separatorChar + k.ToString() + MCParameters.separatorChar + coefficient;
                    sw1.WriteLine(trackableCoefficient);
                    k++;
                }
            }

            //we need an out file for every version
            string outFileName;

            switch (MCParameters.model)
            {
                case 1: // PERSiST 1.4
                case 8: //PERSiST 1.6
                case 10: // PERSiST v.2
                    outFileName = MCParameters.PERSiSTOutputFileName;
                    break;
                 default:
                    outFileName = MCParameters.DefaultINCAOutputFileName;
                    break;
            }
                    
            string[] results = File.ReadAllLines(outFileName);
            int j=0;

            //write to several files to deal with large file size problems
            int m = runNumber % MCParameters.splitsToUse;
            string resultFileName = MCParameters.resultFileNameStub + m.ToString() + ".txt";
            using (StreamWriter sw = File.AppendText(resultFileName))
            {
                foreach (string result in results)
                {
                    string trackableResult = runNumber.ToString() + ",\t " + j.ToString() + ",\t " + result;
                    sw.WriteLine(trackableResult);
                    j++;
                }
            }

            //save the INCA output from PERSiST runs
            // need to fix this for other PERSiST versions
            //note that some tricks are needed to deal with the possibility of multiple input data series
            if (MCParameters.model == 1 ^ MCParameters.model==8 ^ MCParameters.model==10 )
            {
                // specialized routine to get ET data for subsequent analysis
                //harvestOutputsForETSummary(runNumber);

                int x = 0;
                //MF20170102 - test output
                //string INCAOutputFileName = string.Concat(MCParameters.INCAOutputFile, x.ToString("000"), ".dat");
                string INCAOutputFileName = string.Concat(MCParameters.INCAOutputFile, ".dat");

                // right now, just check for file existence and continue.
                while (File.Exists(INCAOutputFileName))
                {
                    Console.WriteLine(INCAOutputFileName);
                    string[] INCAResults = File.ReadAllLines(INCAOutputFileName);
                    int v = 0;

                    //write to several files to deal with large file size problems
                    int w = runNumber % MCParameters.splitsToUse;
                    string INCADatFileName = MCParameters.INCAFileNameStub + x.ToString("000") + "_" + w.ToString("000") + ".txt";
                    using (StreamWriter sw = File.AppendText(INCADatFileName))
                    {
                        foreach (string result in INCAResults)
                        {
                            string trackableResult = runNumber.ToString() + "\t " + v.ToString() + "\t " + result;
                            sw.WriteLine(trackableResult);
                            v++;
                        }
                    }
                    x++;
                    INCAOutputFileName = string.Concat(MCParameters.INCAOutputFile, x.ToString("000"), ".dat");
                }
            }
        }

        static void deleteResults()
        {
            string[] resultFiles = Directory.GetFiles(Directory.GetCurrentDirectory(), MCParameters.resultFileNameStub + "*.txt");
            foreach (string f in resultFiles)
            {
                try { File.Delete(f); }
                catch (Exception ex) { Console.WriteLine(ex.Message); }
            }
        }

        static void deleteINCAInputsFromPERSiST()
        {
            string[] resultFiles = Directory.GetFiles(Directory.GetCurrentDirectory(), MCParameters.INCAFileNameStub + "*.txt");
            foreach (string f in resultFiles)
            {
                try { File.Delete(f); }
                catch (Exception ex) { Console.WriteLine(ex.Message); }
            }
        }
        public static void noteSuccessfulCompletion()
        {
            //write a note of successful completion
            if (!File.Exists("SuccessfulCompletion.txt"))
            {
                // Create a file to write to. 
                using (StreamWriter sw = File.CreateText("SuccessfulCompletion.txt"))
                {
                    sw.WriteLine("Successful Completion");
                    sw.WriteLine("Date: {0}", System.DateTime.Now.ToString());
                }
            }
        }

        static void harvestOutputsForETSummary(int runNumber)
            //this is hard coded currently for the Spanish study
            //ideally at a later point it can be generalized
        {
            string[] landCoverTypes = { "UE", "UD", "RE", "RD" };
            string[] buckets = { "Q", "QR", "S", "SR", "G" };

            if(runNumber==0)
            {
                foreach (string bucket in buckets)
                {
                    Console.WriteLine("Deleting " + bucket);
                    File.Delete("Summary_" + bucket + ".csv");
                }
            }
            foreach (string bucket in buckets)
            {
                string summaryFileName = "Summary_" + bucket + ".csv";

                foreach (string landCoverType in landCoverTypes)
                {
                    string fileName = "PERSiST_" + bucket + "_" + landCoverType +".csv";
                    int v = 0;

                    //get the number of lines in the summary file
                    long u;
                    try
                    {
                        u = File.ReadLines(summaryFileName).Count();
                    }
                    catch (Exception)
                    {

                        u = 0;
                    }

                    if(File.Exists(fileName))
                    {
                        string[] results = File.ReadAllLines(fileName);

                        using (StreamWriter sw = File.AppendText(summaryFileName))
                        {
                            foreach (string result in results)
                            {
                                string trackableResult = u.ToString() + "," + runNumber.ToString() + "," + v.ToString() + "," + result;
                                sw.WriteLine(trackableResult);
                                u++;
                                v++;
                            }
                        }
                    }
                }
            }
       }
    }
}
