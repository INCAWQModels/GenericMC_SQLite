using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace MC
{
    internal static class MCParameters
    {
        internal static int model = 1;
        
        internal enum modelID : int
        {
            persist14   = 1,
            inca_c_1x   = 2,
            inca_PEco   = 3,
            inca_P      = 4,
            inca_Tox    = 5,
            inca_Path   = 6,
            inca_Hg     = 7,
            persist16   = 8,
            inca_ONTHE  = 9,
            persist_2   = 10,
            inca_c_2x   = 11,
            inca_N_1x   = 12
        }
        
        internal static string modelName()
        {
            string modelName;
            //InteractWithModel.availableModels.Find(i => i.p )
            switch (MCParameters.model)
            {
                case 1:
                    modelName = "persist";
                    break;
                case 2:
                    modelName = "inca_c";
                    break;
                case 3:
                    modelName = "inca_PECo";
                    break;
                case 4:
                    modelName = "inca_p";
                    break;
                case 5:
                    modelName = "inca_contaminants";
                    break;
                case 6:
                    modelName = "inca_path";
                    break;
                case 7:
                    modelName = "inca_hg";
                    break;
                case 8:
                    modelName = "persist_1_6";
                    break;
                case 9:
                    modelName = "inca_on_the";
                    break;
                case 10:
                    modelName = "persist_2";
                    break;
                case 11:
                    modelName = "inca_c_2";
                    break;
                case 12:
                    modelName = "inca_N";
                    break;
                default:
                    modelName = "unspecified";
                    break;
            }
            return modelName;
        }

        internal static int persistModelVersionID = 1;

        internal static char separatorChar = ',';
        internal static char dot = '.';
        internal static string minParFileStub = "_min.par";
        internal static string maxParFileStub = "_max.par";
        internal static string minParFile() { return modelName() + minParFileStub; }
        internal static string maxParFile() { return modelName() + maxParFileStub; }

        internal static string MCParFile = "mc.par";
        internal static string bestParSetFileName = "mc.par"; // need this for model initialization

        internal static string INCASummaryFile = "INCASummary.txt";
        internal static string resultFileNameStub ="results";
        internal static string INCAFileNameStub="INCA_";
        internal static string PERSiSTOutputFileName = "PERSiST_streamflow.csv";
        internal static string DefaultINCAOutputFileName = "INCA_out.dsd";
        internal static string PERSiSTOutputFile = "PERSiSTResults.txt";

        //need to initialize observed file name to something
        internal static string observedFileName = "obs.obs";

        //change this back to small after the ET Harvest simulations
        internal static string outputSize = "medium";

        internal static string coefficientsFile = "PERSiST_Errors.csv";
        internal static string coefficientsSummaryFile = "coefficients.txt";
        internal static string coefficientsWeightFile = "coefficientWeights.txt";
        internal static string INCAOutputFile = "INCAOut";
        internal static string LogFileBestPerformance = "logBestPerformance.txt";

        internal static string parameterNameListFile = "parNames.csv";
        internal static string parameterValueListFile = "parList.csv";
        internal static string parameterArrayFileName = "pars.csv";

        internal static double defaultScalingFactorForJump = 0.1;
        internal static double testPerformanceAdjustmentFactor = 1;

        internal static long maxJumps = 2500;
        internal static int maxUnsuccessfulJumps = 50;

        //use for storing the best performance ID
        internal static long bestPerformanceID = 0;
        //use for storing the number of times 
        internal static int freshStarts = 0;

        internal static int maxTries = 300;

        //these need to be made user-definable
        internal static int numberOfLandUses = 6;
        internal static int numberOfReaches = 2;
        internal static int numberOfBoxes = 3;
        internal static int numberOfSedimentClasses = 5;
        internal static int numberOfContaminants = 1;

        internal static int GLUE = 0;

        internal static int coefficientNumber = 2;

        internal static int runsToOrganize = 500;

        internal static double testVal = -1e10; //target test value for starting MC loops

        internal static int splitsToUse = 20; //number of groups to break up output

        internal static double[] seriesWeights;

        internal static double[] coefficientsWeights;

        internal static ArrayList listOfCatchmentNames = new ArrayList();   //list of catchments for PERSiST

        internal static ArrayList listOfContaminants = new ArrayList(); // list of contaminant names for INCA-Tox

        //parameters for database access
        internal static string databaseFileName = ".\\mc.accdb";
    }
}
