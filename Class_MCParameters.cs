using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace MC
{
    public static class MCParameters
    {
        public static int model = 1;
        
        public enum modelID : int
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
        
        public static string modelName()
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

        public static int persistModelVersionID = 1;

        public static char separatorChar = ',';
        public static char dot = '.';
        public static string minParFileStub = "_min.par";
        public static string maxParFileStub = "_max.par";
        public static string minParFile() { return modelName() + minParFileStub; }
        public static string maxParFile() { return modelName() + maxParFileStub; }

        public static string MCParFile = "mc.par";
        public static string bestParSetFileName = "mc.par"; // need this for model initialization

        public static string resultFileNameStub ="results";
        public static string INCAFileNameStub="INCA_";
        public static string PERSiSTOutputFileName = "PERSiST_streamflow.csv";
        public static string DefaultINCAOutputFileName = "INCA_out.dsd";

        //need to initialize observed file name to something
        public static string observedFileName = "obs.obs";

        //change this back to small after the ET Harvest simulations
        public static string outputSize = "medium";

        public static string coefficientsFile = "PERSiST_Errors.csv";
        public static string coefficientsSummaryFile = "coefficients.txt";
        public static string coefficientsWeightFile = "coefficientWeights.txt";
        public static string INCAOutputFile = "INCAOut";
        public static string LogFileBestPerformance = "logBestPerformance.txt";

        public static string parameterNameListFile = "parNames.csv";
        public static string parameterValueListFile = "parList.csv";
        public static string parameterArrayFileName = "pars.csv";

        public static double defaultScalingFactorForJump = 0.1;
        public static double testPerformanceAdjustmentFactor = 1;

        public static long maxJumps = 2500;
        public static int maxUnsuccessfulJumps = 50;

        //use for storing the best performance ID
        public static long bestPerformanceID = 0;
        //use for storing the number of times 
        public static int freshStarts = 0;

        public static int maxTries = 300;

        //these need to be made user-definable
        public static int numberOfLandUses = 6;
        public static int numberOfReaches = 2;
        public static int numberOfBoxes = 3;
        public static int numberOfSedimentClasses = 5;
        public static int numberOfContaminants = 1;

        public static int GLUE = 0;

        public static int coefficientNumber = 2;

        public static int runsToOrganize = 500;

        public static double testVal = -1e10; //target test value for starting MC loops

        public static int splitsToUse = 20; //number of groups to break up output

        public static double[] seriesWeights;

        public static double[] coefficientsWeights;

        public static ArrayList listOfCatchmentNames = new ArrayList();   //list of catchments for PERSiST

        public static ArrayList listOfContaminants = new ArrayList(); // list of contaminant names for INCA-Tox

        //parameters for database access
        public static string databaseFileName = ".\\mc.accdb";
    }
}
