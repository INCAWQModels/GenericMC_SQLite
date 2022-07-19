using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace MC
{

    public class MCMCController : ArrayList
    {
        public int unsuccessfulJumps;
        static Random r = new Random();
        
        public CommandString cs;

        static void savePerformanceIndex(long runNumber, long maxIterations, long bestPerformanceID, double bestModelPerformance)
        {
            StreamWriter sw;
          
            if (runNumber == 0)
                sw = File.CreateText(MCParameters.LogFileBestPerformance);
            else
                sw = File.AppendText(MCParameters.LogFileBestPerformance);
            string ResultString = string.Concat(runNumber.ToString(), MCParameters.separatorChar,  bestPerformanceID.ToString(),MCParameters.separatorChar,maxIterations.ToString(),
                MCParameters.separatorChar,bestModelPerformance);
            Console.WriteLine(ResultString);
            sw.WriteLine(ResultString);
            sw.Close();
        }

        static string returnParFileName()
        {
            if (!File.Exists(MCParameters.MCParFile))
            {
                return MCParameters.bestParSetFileName;
            }
            else
            {
                return MCParameters.MCParFile;
            }
        }

        public static void MCNew(string fileName, string commandLine)
        {
            double bestModelPerformance=-1000;
            double testModelPerformance=-1000;
            double performanceRatio;
            Random acceptanceTest = new Random();

            parameterSet m = new parameterSet(returnParFileName(), MCParameters.minParFile(), MCParameters.maxParFile());

            //use the manual calibration as a starting point
            InteractWithModel.RunModel(commandLine);
            m.modelPerformance = InteractWithModel.EvaluatePerformanceStatistics();
            GLUEAccounting.SaveGLUEResults(m);

            if(double.IsNaN(m.modelPerformance))
            {
                Console.WriteLine("Something has gone horribly wrong - model returned NaN so I will stop now");
                System.Environment.Exit(-1);
            }
            else
            {
                bestModelPerformance = m.modelPerformance;
                testModelPerformance = m.modelPerformance;
            }

            double testVal;
            do
            {
                m.randomizeValues();
                testVal = Math.Sqrt(r.NextDouble());
                m.write(MCParameters.MCParFile);
                if (!File.Exists(MCParameters.MCParFile))
                {
                    Console.WriteLine("******** Unable to find parameter file *****************");
                    File.Copy(MCParameters.bestParSetFileName, MCParameters.MCParFile,true);
                }       
                InteractWithModel.RunModel(commandLine);
                m.modelPerformance = InteractWithModel.EvaluatePerformanceStatistics();
                //GLUEAccounting.saveGLUEResults(m);
                Console.WriteLine("\t*** Comparison -Current: {0:F4} Best : {1:F4}  Test: {2:F4} {3:F4}", m.modelPerformance, bestModelPerformance,
                        bestModelPerformance / m.modelPerformance, testVal);
                //sometimes get current performance better than best                
            }
            while ((bestModelPerformance / m.modelPerformance) < testVal);

            MCMCController MCMC = new MCMCController(m);

            if (File.Exists(fileName))
            {
                try { File.Delete(fileName); }
                catch { Console.WriteLine("Could not delete {0}", fileName); }
            }

            parameterSet min = new parameterSet(MCParameters.minParFile());
            parameterSet max = new parameterSet(MCParameters.maxParFile());

            MCMC.makeMCMCJumpProposal(MCParameters.defaultScalingFactorForJump, m);

            //this is the place to make parallel
            for (var i = 0; i < MCParameters.maxTries; i++)
            {

                //int test=doMCStuff(i);

                int k = 0;
                // not pretty but useful for testing
                bestModelPerformance = MCParameters.testVal;
                MCMC.unsuccessfulJumps = MCParameters.maxUnsuccessfulJumps + 1;

                do
                {
                    k++;
                    // fix this loop so that it jumps out of poorly performing situations
                    if (MCMC.unsuccessfulJumps > MCParameters.maxUnsuccessfulJumps)
                    {
                        bool keepGoing = false;
                        int iterations = 0;
                        do
                        {
                            iterations++;
                            m.randomizeValues();
                            m.write(MCParameters.MCParFile);
                            InteractWithModel.RunModel(commandLine);
                            m.modelPerformance = InteractWithModel.EvaluatePerformanceStatistics();
                            if (MCParameters.model == 2)
                            {
                                GLUEAccounting.SaveGLUEResults(m);
                            }

                            //be quite restrictive with thetest value, doubl square root
                            testVal = Math.Sqrt(r.NextDouble());
                            testVal = Math.Sqrt(testVal);
                            Console.WriteLine("Current: {0:F3} Best : {1:F3}  Performance Ratio: {2:F4} Test: {3:F4}", m.modelPerformance, bestModelPerformance,
                                bestModelPerformance / m.modelPerformance, testVal);
                            //save new best performances where relevant
                            if (m.modelPerformance > bestModelPerformance)
                            {
                                MCParameters.bestParSetFileName = "bestParSet" + i.ToString() + ".par";
                                MCParameters.bestPerformanceID = k;
                                m.write(MCParameters.bestParSetFileName);
                                bestModelPerformance = m.modelPerformance;
                            }

                            //test based on model performance
                            if ((bestModelPerformance / m.modelPerformance) < testVal)
                                keepGoing = true;
                            else
                            {
                                //Console.WriteLine("Possible problems with best model performance {0}",iterations); // for debugging
                                if (iterations > 1)
                                    keepGoing = false;
                            }
                            //test based on a long running loop, jump out if too many iterations
                            if (iterations > MCParameters.maxUnsuccessfulJumps)
                            {
                                keepGoing = false;
                                Console.WriteLine("I think I have made too many jumps");    // for debugging
                                //start with the previous best performing par set
                                if (File.Exists(MCParameters.bestParSetFileName))
                                {
                                    File.Copy(MCParameters.bestParSetFileName, MCParameters.MCParFile, true);
                                }
                                else
                                {
                                    parameterSet mTmp = new parameterSet(returnParFileName(), MCParameters.minParFile(), MCParameters.maxParFile());
                                    mTmp.write(MCParameters.MCParFile);
                                }
                            }
                        }
                        while (keepGoing);
                        keepGoing = true;
                        MCMC.unsuccessfulJumps = 0;
                        m = new parameterSet(MCParameters.MCParFile, MCParameters.minParFile(), MCParameters.maxParFile());
                        m.write(MCParameters.MCParFile);
                        InteractWithModel.RunModel(commandLine);
                                               
                        testModelPerformance = InteractWithModel.EvaluatePerformanceStatistics();
                    }
                    m.MCMC_Sample(updateType.add, MCMC);
                    m.write(MCParameters.MCParFile );
                    if(!File.Exists(MCParameters.MCParFile))
                    {
                        File.Copy(MCParameters.bestParSetFileName, MCParameters.MCParFile,true);
                    }

                    InteractWithModel.RunModel(commandLine);
                    double lastModelPerformance = m.modelPerformance;
                    m.modelPerformance = InteractWithModel.EvaluatePerformanceStatistics();
                

                    //needed for traditional Metropolis Hastings reversible jump
                    //as we can assume the test statistics are always negative, performance ratio
                    //can be set equal to previous model performance / current model performance
                    performanceRatio = bestModelPerformance / m.modelPerformance;

                    //set the acceptance ratio quite high, this will eventually need dynamic parameters
                    double testPerformance;
                    testPerformance = acceptanceTest.NextDouble();
                    testPerformance = Math.Pow(testPerformance, MCParameters.testPerformanceAdjustmentFactor);

                    //put in another adjustment where the performance ratio has to be sufficiently high to accept a jump
                    //this can be done by calling acceptanceTest again

                    if (m.modelPerformance > testModelPerformance)
                    {
                        testModelPerformance = m.modelPerformance;
                        if (m.modelPerformance > bestModelPerformance)
                        {
                            MCMC.unsuccessfulJumps = 0;
                            MCParameters.bestParSetFileName = "bestParSet" + i.ToString() + ".par";
                            MCParameters.bestPerformanceID = k;
                            m.write(MCParameters.bestParSetFileName);
                            bestModelPerformance = m.modelPerformance;
                        }
                    }
                    //rollback a poor performance
                    // check to see if the ratio of current to last model performance is less than a random threshold
                    //make a random jump from either the accepted or current position
                   
                    double localModelPerformanceStatistic = lastModelPerformance / m.modelPerformance;
                    double localTestPerformance = acceptanceTest.NextDouble();
                    Console.WriteLine("{0} {1} {2} {3:F4} {4:F4} {5:F4}", i, k, MCMC.unsuccessfulJumps, performanceRatio, localTestPerformance, m.modelPerformance);
                    //we will evaluate this jump
                    if (performanceRatio > localTestPerformance)
                    {
                        //check that the ratio is good enough
                        if (localModelPerformanceStatistic > 1)
                            m.MCMC_Sample(updateType.add, MCMC);
                        else
                        {
                            m.MCMC_Sample(updateType.subtract, MCMC);
                            //double jumpDistance = 0.05 + MCMC.unsuccessfulJumps / MCParameters.maxUnsuccessfulJumps;
                            //MCMC.makeMCMCJumpProposal(jumpDistance, m);
                            MCMC.makeMCMCJumpProposal(MCParameters.defaultScalingFactorForJump, m);
                            MCMC.unsuccessfulJumps++;
                        }
                    }
                    else
                    {
                        MCMC.unsuccessfulJumps++;
                        m.randomizeValues();
                    }
                    m.write(MCParameters.MCParFile);
                    lastModelPerformance = m.modelPerformance;
                }
                while (k < MCParameters.maxJumps);
                savePerformanceIndex(i, k, MCParameters.bestPerformanceID,bestModelPerformance);
            }
        }



        public MCMCController(parameterSet pSet)
        {
            unsuccessfulJumps = 0;

            //initialize the jumps
            foreach (ArrayList l in pSet)
            {
                //an arraylist may be overkill here but it keeps things consistent
                double[] CandidateJumps = new double[l.Count];
                int arrayCounter = 0;
                foreach (parameter p in l)
                {
                    CandidateJumps[arrayCounter++]=0.0;
                }
                this.Add(CandidateJumps);
            }
        }

        public void makeMCMCJumpProposal(double scalingFactor,parameterSet pSet)
        {
            for (int i = 0; i < this.Count; i++)
            {
                ArrayList paramArray = (ArrayList)pSet[i];
                double[] jumps = (double[])this[i];

                for (int j = 0; j < paramArray.Count; j++)
                {
                    parameter p = (parameter)paramArray[j];
                    jumps[j]= p.CandidateMCMCJump(scalingFactor);
                }
            }
        }
    }
}
