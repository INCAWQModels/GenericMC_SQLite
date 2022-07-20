using System;
using System.IO;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;


namespace MC
{
    class Program
    {
        static void Main()
        {
            string s;
            int i;
            Console.WriteLine("******************************");
            Console.WriteLine("* Choose 1 for MCTool        *");
            Console.WriteLine("* Choose 2 for GLUE          *");
            Console.WriteLine("* Any other key to exit      *");
            Console.WriteLine("******************************");
            s=Console.ReadLine();
            int.TryParse(s, out i);

            MCParameters.GLUE = i;

            switch (i)
            {
                case 1:
                    MC_Main();
                    break;
                case 2:
                    GLUE_Main();
                    break;
                default:
                    Console.WriteLine("No valid argument provided, exiting");
                    break;
            }
        }

        static void MC_Main()
        {
            //remove the successful completion flag
            if (File.Exists("SuccessfulCompletion.txt"))
            {
                File.Delete("SuccessfulCompletion.txt");
            }
            
             InteractWithModel.WhatModel();
            
            //check that it will be possible to write to a database
            resultsDatabase MCResults = new resultsDatabase();
            
            //use the commandString to get all the arguments
            CommandString cs = new CommandString();
            //use runString to get the right set of output files written, "-size none" during runs
            String runString;

            InteractWithModel.SetRunMCParameters();
 
            cs.Populate();

            //this will need to be updated as more models are added
            switch(MCParameters.model)
            {
                case 1:
                case 8:
                case 10:
                    MCParameters.coefficientsFile = "PERSiST_Errors.csv";
                    break;
                default:
                    MCParameters.coefficientsFile = "coefficients.csv";
                    break;
            }
            
            InteractWithModel.SetLandUseAndReaches();

            //run the model once to ensure outputs exist
            runString = string.Concat(cs.commandLine, " -size none");
            Console.WriteLine(runString);
            
            InteractWithModel.RunModel(runString);

            InteractWithModel.SetCoefficientWeights();
            InteractWithModel.WriteCoefficientWeights();
            InteractWithModel.SetSeriesWeights();
            // 
            // sometimes default parameter file does not exist, causes MCNew to fail
            //
            MCMCController.MCNew("mc.csv", runString);
            MCParameters.runsToOrganize = MCParameters.maxTries;

            //now we need some output
            //so we need to reset the runstring
            
            runString = string.Concat(cs.commandLine, " -size ", MCParameters.outputSize, " ");
            //may be needed
            if (MCParameters.model == 1 ^ MCParameters.model == 8 ^ MCParameters.model==10 )
            {
                runString=string.Concat(runString, " -inca ", MCParameters.INCAOutputFile );
            }

            /*
            MCResults.createParameterSensitivitySummaryTable();
            Console.WriteLine("Parameter sensitivity summary created about to write coefficient weights");
            */
            MCResults.writeCoefficientWeights();
            Console.WriteLine("Coefficient weights written");
            SummarizeResults.write(runString,MCResults);

            SummarizeResults.noteSuccessfulCompletion();
            Console.ReadLine();
        }
        
        static void GLUE_Main()
        {
            //remove the successful completion flag
            if (File.Exists("SuccessfulCompletion.txt"))
            {
                File.Delete("SuccessfulCompletion.txt");
            }
            InteractWithModel.WhatModel();

            //use the commandString to get all the arguments
            CommandString cs = new CommandString();
            //use runString to get the right set of output files written, "-size none" during runs
            String runString;

            InteractWithModel.SetRunMCParameters();

            cs.Populate();

            //this will need to be updated as more models are added
            Console.WriteLine(MCParameters.modelID.inca_c_1x);

            switch(MCParameters.model)
            {
                case 1:
                case 8:
                case 10:
                    {
                        MCParameters.coefficientsFile = "PERSiST_Errors.csv";
                        break;
                    }
                default:
                    {
                        MCParameters.coefficientsFile = "coefficients.csv";
                        break;
                    }
            }
                
            InteractWithModel.SetLandUseAndReaches();

            //run the model once to ensure outputs exist
            runString = string.Concat(cs.commandLine, " -size none");
            Console.WriteLine(runString);

            InteractWithModel.RunModel(runString);
            InteractWithModel.SetCoefficientWeights();
            InteractWithModel.WriteCoefficientWeights();
            InteractWithModel.SetSeriesWeights();
            
            for(long i=0;i<12500; i++)
            {
                GLUEAccounting.GLUERun(runString, i);
            }

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
    }

}
