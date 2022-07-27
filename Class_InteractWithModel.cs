using System;
using System.IO;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace MC
{
    /// <summary>
    /// Code to interact with INCA executable
    /// </summary>
    public class InteractWithModel
    {
        /// <summary>
        /// wrapper structure with model name, id and version
        /// </summary>
        public struct ModelStruct
        {
            public int id;
            public string name;
            public string version;

            public ModelStruct(int idToUse, string nameToUse, string versionToUse)
            {
                id = idToUse;
                name = nameToUse;
                version = versionToUse;
            }

        };

        public static List<ModelStruct> availableModels;
       

        /// <summary>
        /// display list of available models to console
        /// </summary>
        public static void WhatModel()
        {
            string r;
            int s;

            addAvailableModels();

            Console.WriteLine();
            Console.WriteLine("**************************************");
            foreach(ModelStruct item in availableModels)
            {
                Console.WriteLine("Enter {0} for {1} version {2} : ", item.id, item.name, item.version);
            }
            r = Console.ReadLine();
            try
            {
                s = Convert.ToInt32(r);
                MCParameters.model = s;
            }
            catch
            {
                Console.WriteLine("You did not enter a valid number, I'm running PERSiST 1.4");
            }
        }

        /// <summary>
        /// run a model at the command line
        /// </summary>
        /// <param name="commandString">command line to run</param>
        public static void RunModel(string commandString)
        {
            try
            {
                System.Diagnostics.ProcessStartInfo procStartInfo =
                    new System.Diagnostics.ProcessStartInfo("cmd", "/c " + commandString)
                    {

                        // The following commands are needed to redirect the standard output.
                        // This means that it will be redirected to the Process.StandardOutput StreamReader.
                        RedirectStandardOutput = true,
                        UseShellExecute = false,
                        // Do not create the black window.
                        CreateNoWindow = true
                    };
                // Now we create a process, assign its ProcessStartInfo and start it
                System.Diagnostics.Process proc = new System.Diagnostics.Process();
                proc.StartInfo = procStartInfo;
                proc.Start();
                // Get the output into a string
                string result = proc.StandardOutput.ReadToEnd();
                //
                // not really pretty but should tag the problem
                //
                if (result.Length > 1)
                {
                    Console.WriteLine(result);
                    File.WriteAllText("error.lst", result);
                    Environment.Exit(-1);
                }
            }
            catch
            {
                Console.WriteLine("Could not run model");
            }
        }

        /// <summary>
        /// return useable Nash Sutcliffe (NS-1) for preformance statistics
        /// </summary>
        /// <param name="s">string containing candidate NS value</param>
        /// <returns>NS for performance statistic calculation</returns>
        protected static double ReturnNS(string s)
        {
            //note that things may behave in a very odd way if NS crosses 0.
            //Maximizing NS-1 will prevent this from happenning
            double val;
            try
            {
                val = Convert.ToDouble(s);
                val -= 1.0;
            }
            catch
            {
                val = 0.0;
            }
            return val;
        }

        /// <summary>
        /// Return useable log(Nash Sutcliffe) log(NS)-1 for performance statistics
        /// </summary>
        /// <param name="s">string containing candidate log(NS) value</param>
        /// <returns>log NS for preformance statistic calculation</returns>
        static double ReturnLogNS(string s)
        {
            //note that things may behave in a very odd way if NS crosses 0.
            //Maximizing NS-1 will prevent this from happenning
            double val;
            try
            {
                val = Convert.ToDouble(s);
                val -= 1.0;
            }
            catch
            {
                val = 0.0;
            }
            return val;
        }

        /// <summary>
        /// Return useable Kling Gupta Efficiency (KGE-1)
        /// </summary>
        /// <param name="s">string containing candidate KGE value</param>
        /// <returns>KGE for performance statistic calculation</returns>
        static double ReturnKGE(string s)
        {
            //note that things may behave in a very odd way if KGE crosses 0.
            //Maximizing KGE-1 will prevent this from happenning
            //Console.WriteLine($"{s}");
            double val;
            try
            {
                val = Convert.ToDouble(s);
                val -= 1.0;
            }
            catch
            {
                val = 0.0;
            }
            return val;
        }
        /// <summary>
        /// Return useable Pearson correlation (R2-1)
        /// </summary>
        /// <param name="s">string containing candidate correlation</param>
        /// <returns>R2 for performance statistic calculation</returns>
        static double ReturnR2(string s)
        {
            double val;
            try
            {
                val = Convert.ToDouble(s);
                val -= 1.0;
            }
            catch
            {
                val = 0.0;
            }
            return val;
        }

        /// <summary>
        /// Return absolute difference (AD) in a useable form for performance statistic calculation
        /// </summary>
        /// <param name="s">string containing candidate AD</param>
        /// <returns>AD for performance statistics</returns>
        static double ReturnAD(string s)
        {
            double val;
            try
            {
                val = Convert.ToDouble(s);
                val = -1.0 * Math.Abs(val);
            }
            catch
            {
                val = 0.0;
            }
            return val;
        }
        
        /// <summary>
        /// return root mean square error (RMSE) in a form suitable for calculating performance statistics
        /// </summary>
        /// <param name="s">string containing candidate RMSE</param>
        /// <returns>RMSE for performance statistics</returns>
        static double ReturnRMSE(string s)
        {
            double val;
            try
            {
                val = Convert.ToDouble(s);
                val = -1.0 * val;
            }
            catch
            {
                val = 0.0;
            }
            return val;
        }

        /// <summary>
        /// return Relative Error (RE) in a format suitable for performance statistics
        /// </summary>
        /// <param name="s">string containing candidate RE</param>
        /// <returns>RE for performance statistics</returns>
        static double ReturnRE(string s)
        {
            double val;
            try
            {
                val = Convert.ToDouble(s);
                val = -1.0 * Math.Abs(val);
            }
            catch
            {
                val = 0.0;
            }
            return val;
        }

        /// <summary>
        /// return a Variance Ratio (VR) for use in calculating performance statistics
        /// </summary>
        /// <param name="s">string containing candidate VR</param>
        /// <returns>VR for use in performance statistics</returns>
        static double ReturnVR(string s)
        {
            double val;
            try
            {
                val = Convert.ToDouble(s);
                if (val > 1.0) { val = 1.0 / val; }
                val -= 1.0;
            }
            catch
            {
                val = 0.0;
            }
            return val;

        }

        /// <summary>
        /// process Limits of Acceptablility performace statistics
        /// </summary>
        /// <param name="s">string containing candidate statistic</param>
        /// <returns>candidate LOA statistic</returns>
        static double ReturnCat(string s)
        {
            double val;
            
            try
            {
                // we still have some "-999" codes in the CatB and CatC output
                if (s.Equals("-999"))
                    val = 0.0;
                else
                    val = Convert.ToDouble(s)-1.0;
            }
            catch
            {
                val = 0.0;
            }
            return val;
        }
     
        /// <summary>
        /// estimate performance statistic (ranges from -infinity to zero) for a model run
        /// </summary>
        /// <returns>performance statistic</returns>        
        public static double EvaluatePerformanceStatistics()
        {
            string fileName = MCParameters.coefficientsFile;
            {
                double performanceStatistic = 0.0;
                int i = 0;

                try
                {
                    foreach (string line in File.ReadLines(fileName))
                    {

                        string trimLine = line.Trim();
                        string[] values = trimLine.Split(',');

                        try
                        {
                            switch (MCParameters.model)
                            {
                                //should be able to calculate KGE for any version of PERSiST
                                case 1:     //running PERSiST 1.4
                                case 8:     //running PERSiST 1.6
                                    //performanceStatistic += MCParameters.seriesWeights[i] * MCParameters.coefficientsWeights[1] * ReturnR2(values[1]);
                                    //performanceStatistic += MCParameters.seriesWeights[i] * MCParameters.coefficientsWeights[2] * ReturnNS(values[2]);
                                    //performanceStatistic += MCParameters.seriesWeights[i] * MCParameters.coefficientsWeights[3] * ReturnLogNS(values[3]);
                                    //performanceStatistic += MCParameters.seriesWeights[i] * MCParameters.coefficientsWeights[6] * ReturnAD(values[6]);
                                    //performanceStatistic += MCParameters.seriesWeights[i] * MCParameters.coefficientsWeights[7] * ReturnVR(values[7]);
                                    //break;
                                case 10:    //running PERSiST 2.0
                                    double r, alphaMinus1, beta, tmp, tmpPs;
                                    tmpPs = MCParameters.seriesWeights[i] * MCParameters.coefficientsWeights[1] * ReturnR2(values[1]);
                                    tmpPs += MCParameters.seriesWeights[i] * MCParameters.coefficientsWeights[2] * ReturnNS(values[2]);
                                    tmpPs += MCParameters.seriesWeights[i] * MCParameters.coefficientsWeights[3] * ReturnLogNS(values[3]);
                                    tmpPs += MCParameters.seriesWeights[i] * MCParameters.coefficientsWeights[6] * ReturnAD(values[6]);
                                    tmpPs += MCParameters.seriesWeights[i] * MCParameters.coefficientsWeights[7] * ReturnVR(values[7]);
                                    //
                                    //calculate KGE from R2, VR and AD
                                    //KGE = 1-sqrt((r-1)^2 + (AD-1)^2  + (VR-1)^2)
                                    //
                                    r = Convert.ToDouble(values[1]);
                                    // only continue if r2 > 0
                                    if (r > 0)
                                    {
                                        //  Check that the weights for R2, AD and VR are all 1 and that weights for NS and logNS are 0
                                        if (Convert.ToInt32(MCParameters.coefficientsWeights[1]) != 1 ||
                                            Convert.ToInt32(MCParameters.coefficientsWeights[2]) != 0 ||
                                            Convert.ToInt32(MCParameters.coefficientsWeights[3]) != 0 ||
                                            Convert.ToInt32(MCParameters.coefficientsWeights[6]) != 1 ||
                                            Convert.ToInt32(MCParameters.coefficientsWeights[7]) != 1)
                                        {
                                            performanceStatistic += tmpPs;
                                        }
                                        else
                                        {
                                            r = Math.Sqrt(r);
                                            alphaMinus1 = Convert.ToDouble(values[6]);    //AD - same as (1-alpha)
                                            beta = Convert.ToDouble(values[7]);     //VR
                                            tmp = (r - 1) * (r - 1);
                                            tmp += alphaMinus1 * alphaMinus1;
                                            tmp += (beta - 1) * (beta - 1);
                                            tmp = Math.Sqrt(tmp);
                                            performanceStatistic -= tmp;
                                            //Console.WriteLine("Calculating KGE : {0} {1} {2} {3} {4}", performanceStatistic,tmp, r,alpha,beta);
                                        }
                                    }
                                    break;
                                case 2:  // running INCA-C
                                case 11: //running INCA-C
                                    performanceStatistic += MCParameters.seriesWeights[i] * MCParameters.coefficientsWeights[1] * ReturnR2(values[1]);
                                    performanceStatistic += MCParameters.seriesWeights[i] * MCParameters.coefficientsWeights[2] * ReturnNS(values[2]);
                                    break;
                                case 3: // running INCA-PECo
                                    //added Kling Gupta
                                    performanceStatistic += MCParameters.seriesWeights[i] * MCParameters.coefficientsWeights[1] * ReturnR2(values[1]);
                                    performanceStatistic += MCParameters.seriesWeights[i] * MCParameters.coefficientsWeights[2] * ReturnNS(values[2]);
                                    performanceStatistic += MCParameters.seriesWeights[i] * MCParameters.coefficientsWeights[3] * ReturnLogNS(values[3]);
                                    performanceStatistic += MCParameters.seriesWeights[i] * MCParameters.coefficientsWeights[4] * ReturnRMSE(values[4]);
                                    performanceStatistic += MCParameters.seriesWeights[i] * MCParameters.coefficientsWeights[5] * ReturnRE(values[5]);
                                    performanceStatistic += MCParameters.seriesWeights[i] * MCParameters.coefficientsWeights[6] * ReturnAD(values[6]);
                                    performanceStatistic += MCParameters.seriesWeights[i] * MCParameters.coefficientsWeights[7] * ReturnVR(values[7]);
                                    performanceStatistic += MCParameters.seriesWeights[i] * MCParameters.coefficientsWeights[11] * ReturnKGE(values[11]);
                                    performanceStatistic += MCParameters.seriesWeights[i] * MCParameters.coefficientsWeights[12] * ReturnCat(values[12]);
                                    performanceStatistic += MCParameters.seriesWeights[i] * MCParameters.coefficientsWeights[13] * ReturnCat(values[13]);
                                    performanceStatistic += MCParameters.seriesWeights[i] * MCParameters.coefficientsWeights[14] * ReturnCat(values[14]);
                                    performanceStatistic += MCParameters.seriesWeights[i] * MCParameters.coefficientsWeights[15] * ReturnCat(values[15]);

                                    //Console.WriteLine("i {0} Performance statistic {1:F} Series weight {2} Coefficient Weight {3} NS {4:F}", i,performanceStatistic, MCParameters.seriesWeights[i], MCParameters.coefficientsWeights[2], returnNS(values[2]));
                                    break;
                                case 4: // running INCA-P
                                    performanceStatistic += MCParameters.seriesWeights[i] * MCParameters.coefficientsWeights[1] * ReturnR2(values[1]);
                                    performanceStatistic += MCParameters.seriesWeights[i] * MCParameters.coefficientsWeights[2] * ReturnNS(values[2]);
                                    performanceStatistic += MCParameters.seriesWeights[i] * MCParameters.coefficientsWeights[5] * ReturnVR(values[5]);
                                    break;
                                case 5: // running INCA-Tox
                                        // need NS + Cat-B + Cat-C
                                    performanceStatistic += MCParameters.seriesWeights[i] * MCParameters.coefficientsWeights[2] * ReturnNS(values[2]);
                                    performanceStatistic += MCParameters.seriesWeights[i] * MCParameters.coefficientsWeights[6] * ReturnCat(values[6]);
                                    performanceStatistic += MCParameters.seriesWeights[i] * MCParameters.coefficientsWeights[7] * ReturnCat(values[7]);
                                    break;
                                case 6: // running INCA-Path
                                    performanceStatistic += MCParameters.seriesWeights[i] * MCParameters.coefficientsWeights[1] * ReturnR2(values[1]);
                                    performanceStatistic += MCParameters.seriesWeights[i] * MCParameters.coefficientsWeights[2] * ReturnNS(values[2]);
                                    performanceStatistic += MCParameters.seriesWeights[i] * MCParameters.coefficientsWeights[3] * ReturnLogNS(values[3]);
                                    performanceStatistic += MCParameters.seriesWeights[i] * MCParameters.coefficientsWeights[7] * ReturnVR(values[7]);
                                    break;
                                case 7: //running INCA-Hg
                                    performanceStatistic += MCParameters.seriesWeights[i] * MCParameters.coefficientsWeights[1] * ReturnR2(values[1]);
                                    performanceStatistic += MCParameters.seriesWeights[i] * MCParameters.coefficientsWeights[2] * ReturnNS(values[2]);
                                    break;
                                case 9: // running INCA_ON(THE)
                                    performanceStatistic += MCParameters.seriesWeights[i] * MCParameters.coefficientsWeights[1] * ReturnR2(values[1]);
                                    performanceStatistic += MCParameters.seriesWeights[i] * MCParameters.coefficientsWeights[2] * ReturnNS(values[2]);
                                    performanceStatistic += MCParameters.seriesWeights[i] * MCParameters.coefficientsWeights[3] * ReturnLogNS(values[3]);
                                    performanceStatistic += MCParameters.seriesWeights[i] * MCParameters.coefficientsWeights[6] * ReturnAD(values[6]);
                                    performanceStatistic += MCParameters.seriesWeights[i] * MCParameters.coefficientsWeights[7] * ReturnVR(values[7]);
                                    performanceStatistic += MCParameters.seriesWeights[i] * MCParameters.coefficientsWeights[11] * ReturnKGE(values[11]);
                                    break;
                                case 12: // running INCA-N
                                    performanceStatistic += MCParameters.seriesWeights[i] * MCParameters.coefficientsWeights[1] * ReturnR2(values[1]);
                                    performanceStatistic += MCParameters.seriesWeights[i] * MCParameters.coefficientsWeights[2] * ReturnNS(values[2]);
                                    performanceStatistic += MCParameters.seriesWeights[i] * MCParameters.coefficientsWeights[5] * ReturnVR(values[5]);
                                    break;
                                default:
                                    Console.WriteLine("This should not happen - invalid model ID code in performance statistic retrieval");
                                    break;
                            }
                        }
                        catch
                        {
                            performanceStatistic += 0.0;
                        }
                        i++;
                        //Console.WriteLine(performanceStatistic);
                    }
                }
                catch (Exception ex) { Console.WriteLine(ex.Message); };
               
                return performanceStatistic;
            }
        }

        /// <summary>
        /// get the weight to use for an individual performance statistic
        /// </summary>
        /// <param name="testStatistic">name of the statistic</param>
        /// <returns>statistic weight</returns>
        static double GetWeight(string testStatistic)
        {
            double weight = 0.0;
            Console.Write("Please enter a weight to use for {0} : ", testStatistic);
            try { weight = Convert.ToDouble(Console.ReadLine()); }
            catch { Console.WriteLine("Invalid weight, setting value to 0.0"); }
            return weight;
        }

        /// <summary>
        /// set coefficient weights for all version of PERSiST
        /// </summary>
        static void SetPERSiSTCoefficientWeights()
        {
            MCParameters.coefficientsWeights[1] = GetWeight("R2 (Pearson Correlation)");
            MCParameters.coefficientsWeights[2] = GetWeight("NS (Nash Sutcliffe)");
            MCParameters.coefficientsWeights[3] = GetWeight("logNS (log(Nash Sutcliffe))");
            MCParameters.coefficientsWeights[6] = GetWeight("AD (Absolute Difference)");
            MCParameters.coefficientsWeights[7] = GetWeight("VR (Variance Ratio)");
        }

        /// <summary>
        /// set coefficient weights for INCA-P classic
        /// </summary>
        static void SetINCA_PCoefficientWeights()
        {
            MCParameters.coefficientsWeights[1] = GetWeight("R2 (Pearson Correlation)");
            MCParameters.coefficientsWeights[2] = GetWeight("NS (Nash Sutcliffe)");
            MCParameters.coefficientsWeights[5] = GetWeight("VR (Variance Ratio)");
        }

        /// <summary>
        /// set coefficient weights for INCA-N classic
        /// </summary>
        static void SetINCA_NCoefficientWeights()
        {
            MCParameters.coefficientsWeights[1] = GetWeight("R2 (Pearson Correlation)");
            MCParameters.coefficientsWeights[2] = GetWeight("NS (Nash Sutcliffe)");
        }
        
        /// <summary>
        /// set coefficient weights for INCA-PEco
        /// </summary>
        static void SetINCA_PEcoCoefficientWeights() //INCA_PEco
        {
            MCParameters.coefficientsWeights[1] = GetWeight("R2 (Pearson Correlation)");
            MCParameters.coefficientsWeights[2] = GetWeight("NS (Nash Sutcliffe)");
            MCParameters.coefficientsWeights[3] = GetWeight("log(NS) (log Nash Sutcliffe)");
            MCParameters.coefficientsWeights[4] = GetWeight("RMSE (Root mean square error)");
            MCParameters.coefficientsWeights[5] = GetWeight("RE (Relative error)");
            MCParameters.coefficientsWeights[6] = GetWeight("AD (Absolute Difference)");
            MCParameters.coefficientsWeights[7] = GetWeight("VR (Variance Ratio)");
            MCParameters.coefficientsWeights[11] = GetWeight("KGE (Kling Gupta Efficiency)");
            MCParameters.coefficientsWeights[12] = GetWeight("CatB");
            MCParameters.coefficientsWeights[13] = GetWeight("CatC");
            MCParameters.coefficientsWeights[14] = GetWeight("CatCa");
            MCParameters.coefficientsWeights[15] = GetWeight("CatCb");
        }

        /// <summary>
        /// set coefficient weights for INCA ON THE
        /// </summary>
        static void SetINCA_ONTHECoefficientWeights()
        {
            MCParameters.coefficientsWeights[1] = GetWeight("R2 (Pearson Correlation)");
            MCParameters.coefficientsWeights[2] = GetWeight("NS (Nash Sutcliffe)");
            MCParameters.coefficientsWeights[3] = GetWeight("log(NS) (log Nash Sutcliffe)");
            MCParameters.coefficientsWeights[6] = GetWeight("AD (Absolute Difference)");
            MCParameters.coefficientsWeights[7] = GetWeight("VR (Variance Ratio)");
            MCParameters.coefficientsWeights[11] = GetWeight("KGE (Kling Gupta Efficiency)");
        }

        /// <summary>
        /// set coefficient weights for INCA-C classic
        /// </summary>
        static void SetINCA_CCoefficientWeights()
        {
            MCParameters.coefficientsWeights[1] = GetWeight("R2 (Pearson Correlation)");
            MCParameters.coefficientsWeights[2] = GetWeight("NS (Nash Sutcliffe)");
        }

        /// <summary>
        /// wrapper code to set coefficient weights for calculating model performance
        /// </summary>
        public static void SetCoefficientWeights()
        {
            //fix this later, currently placeholder code to add coefficient weights
            MCParameters.coefficientsWeights = new double[20];
            for (int i = 0; i < 20; i++)
                MCParameters.coefficientsWeights[i] = 1.0;

            Console.WriteLine("Model Number {0}", MCParameters.model);

            switch(MCParameters.model)
            {
                case 1:  //PERSiST 1.4
                case 8:  //PERSiST 1.6
                case 10: //PERSiST 2.0
                    SetPERSiSTCoefficientWeights();
                    break;
                case 2: //INCA-C
                    SetINCA_CCoefficientWeights();
                    break;
                case 3: //INCA-PECo
                    SetINCA_PEcoCoefficientWeights();
                    break;
                case 4: //INCA-P
                    SetINCA_PCoefficientWeights();
                    break;
                case 9: //INCA(ON)THE
                    SetINCA_ONTHECoefficientWeights();
                    break;
                case 12: // INCA-N Classic
                    SetINCA_NCoefficientWeights();
                    break;
                default:
                    break;
            }
        }

        /// <summary>
        /// save coefficient weights to a file
        /// </summary>
        public static void WriteCoefficientWeights()
        {
            switch (MCParameters.model)
            {
                case 1:  //PERSiST 1.4
                case 8:  //PERSiST 1.6
                case 10: //PERSiST 2.0
                    WritePERSiSTCoefficientWeights();
                    break;
                case 2:
                    WriteINCA_CCoefficientWeights();
                    break;
                case 3: //INCA-PEco
                    WriteINCA_PEcoCoefficientWeights();
                    break;
                case 4: //INCA-P
                    WriteINCA_PCoefficientWeights();
                    break;
                case 9: //INCA(ON)THE
                    WriteINCA_ONTHECoefficientWeights();
                    break;
                case 12:
                    WriteINCA_NCoefficientWeights();
                    break;
                default:
                    break;
            }
        }

        /// <summary>
        /// save PERSiST coefficient weights to file
        /// </summary>
        static void WritePERSiSTCoefficientWeights()
        {
            //overwrite existing file
            using var cwf = new StreamWriter(MCParameters.coefficientsWeightFile, false);
            cwf.WriteLine("R2 (Pearson Correlation), {0}", MCParameters.coefficientsWeights[1]);
            cwf.WriteLine("NS (Nash Sutcliffe), {0}", MCParameters.coefficientsWeights[2]);
            cwf.WriteLine("logNS (log(Nash Sutcliffe)), {0}", MCParameters.coefficientsWeights[3]);
            cwf.WriteLine("AD (Absolute Difference), {0}", MCParameters.coefficientsWeights[6]);
            cwf.WriteLine("VR (Variance Ratio), {0}", MCParameters.coefficientsWeights[7]);
        }

        /// <summary>
        /// save INCA-PECO coefficient weights to file
        /// </summary>
        static void WriteINCA_PCoefficientWeights()
        {
            //overwrite existing file
            using (StreamWriter cwf = new StreamWriter(MCParameters.coefficientsWeightFile, false))
            {
                cwf.WriteLine("R2 (Pearson Correlation), {0}", MCParameters.coefficientsWeights[1]);
                cwf.WriteLine("NS (Nash Sutcliffe), {0}", MCParameters.coefficientsWeights[2]);
                cwf.WriteLine("VR (Variance Ratio), {0}", MCParameters.coefficientsWeights[5]);
            }
        }

        /// <summary>
        /// save INCA-N classic coefficient weights to file
        /// </summary>
        static void WriteINCA_NCoefficientWeights()
        {
            //overwrite existing file
            using (StreamWriter cwf = new StreamWriter(MCParameters.coefficientsWeightFile, false))
            {
                cwf.WriteLine("R2 (Pearson Correlation), {0}", MCParameters.coefficientsWeights[1]);
                cwf.WriteLine("NS (Nash Sutcliffe), {0}", MCParameters.coefficientsWeights[2]);
            }
        }

        /// <summary>
        /// write INCA-C (classic ?) coefficient weights to file
        /// </summary>
        static void WriteINCA_CCoefficientWeights()
        {
            //overwrite existing file
            using (StreamWriter cwf = new StreamWriter(MCParameters.coefficientsWeightFile, false))
            {
                cwf.WriteLine("R2 (Pearson Correlation), {0}", MCParameters.coefficientsWeights[1]);
                cwf.WriteLine("NS (Nash Sutcliffe), {0}", MCParameters.coefficientsWeights[2]);
                cwf.WriteLine("VR (Variance Ratio), {0}", MCParameters.coefficientsWeights[5]);
            }
        }
        /// <summary>
        /// write INCA ON THE performance statistic weights to file
        /// </summary>
        static void WriteINCA_ONTHECoefficientWeights()
        {
            //overwrite existing file
            using (StreamWriter cwf = new StreamWriter(MCParameters.coefficientsWeightFile, false))
            {
                cwf.WriteLine("R2 (Pearson Correlation), {0}", MCParameters.coefficientsWeights[1]);
                cwf.WriteLine("NS (Nash Sutcliffe), {0}", MCParameters.coefficientsWeights[2]);
                cwf.WriteLine("logNS (log(Nash Sutcliffe)), {0}", MCParameters.coefficientsWeights[3]);
                cwf.WriteLine("AD (Absolute Difference), {0}", MCParameters.coefficientsWeights[6]);
                cwf.WriteLine("VR (Variance Ratio), {0}", MCParameters.coefficientsWeights[7]);
                cwf.WriteLine("KGE (Kling Gupta Efficiency), {0}", MCParameters.coefficientsWeights[11]);
            }
        }


        //will need some editing
        /// <summary>
        /// first try at writing INCA-PEco coefficients to file
        /// </summary>
        static void WriteINCA_PEcoCoefficientWeights()
        {
            //overwrite existing file
            using (StreamWriter cwf = new StreamWriter(MCParameters.coefficientsWeightFile, false))
            {
                cwf.WriteLine("R2 (Pearson Correlation), {0}", MCParameters.coefficientsWeights[1]);
                cwf.WriteLine("NS (Nash Sutcliffe), {0}", MCParameters.coefficientsWeights[2]);
                cwf.WriteLine("RMSE (Root Mean Square Error), {0}", MCParameters.coefficientsWeights[3]);
                cwf.WriteLine("RE (Relative Error), {0}", MCParameters.coefficientsWeights[4]);
                cwf.WriteLine("VR (Variance Ratio), {0}", MCParameters.coefficientsWeights[5]);
                cwf.WriteLine("CatB (CatB), {0}", MCParameters.coefficientsWeights[6]);
                cwf.WriteLine("CatC (CatC), {0}", MCParameters.coefficientsWeights[7]);
                cwf.WriteLine("CatCa (CatCa), {0}", MCParameters.coefficientsWeights[8]);
                cwf.WriteLine("CatCb (CatCb), {0}", MCParameters.coefficientsWeights[9]);
            }
        }

        // this needs to be given some thought and fixed
        static void _setSeriesWeights(string fileName)
        {
            ArrayList l = new ArrayList();
            long count = 0;

            using (StreamReader r = new StreamReader(fileName))
            {
                string line;
                while ((line = r.ReadLine()) != null)
                {
                    l.Add(line);
                    count++;
                }
            }
            MCParameters.seriesWeights = new double[count];
            long n = 0;

            foreach (string m in l)
            {
                Console.Write("{0} : ", m);
                string k = Console.ReadLine();
                try     { MCParameters.seriesWeights[n] = Convert.ToDouble(k); }
                catch   { MCParameters.seriesWeights[n] = 0.0; }
                n++;
            }
        }

        /// <summary>
        /// wrapper code to set weights for individual parameters
        /// </summary>
        /// <returns></returns>
        public static int SetSeriesWeights()
        {
            Console.WriteLine();
            Console.WriteLine("*********************************");
            Console.WriteLine();
            try
            {
                //debug again
                _setSeriesWeights(MCParameters.coefficientsFile);
                return 0;
            }
            catch { return -1; }
        }

        /// <summary>
        /// get the number of substances to use in an INCA-Contaminants simulation
        /// </summary>
        public static void SetNumberOfContaminants()
        {
            string r;
            int s;
            
            Console.WriteLine("**********************************\n\n");
            Console.Write("Please enter the number of contaminants : ");
            r = Console.ReadLine();
            try
            {
                s = Convert.ToInt32(r);
                MCParameters.numberOfContaminants = s;
            }
            catch
            {
                Console.WriteLine("You did not enter a valid number, the number of contaminants has been set to {0}", MCParameters.numberOfContaminants);
            }
        }

        /// <summary>
        /// get the number of land use types and reaches to use in a model run
        /// </summary>
        public static void SetLandUseAndReaches()
        {
            string r;
            int s;

            Console.WriteLine("**********************************\n\n");
            Console.Write("Please enter the number of land uses (6 for standard INCA) : ");
            r = Console.ReadLine();
            try
            {
                s = Convert.ToInt32(r);
                MCParameters.numberOfLandUses = s;
            }
            catch 
            {
                Console.WriteLine("You did not enter a valid number, the number of land uses has been set to {0}", MCParameters.numberOfLandUses);
            }
            Console.Write("Please enter the nunber of reaches : ");
            r = Console.ReadLine();
            try
            {
                s = Convert.ToInt32(r);
                MCParameters.numberOfReaches = s;
            }
            catch
            {
                Console.WriteLine("You did not enter a valid number, the number of reaches has been set to {0}", MCParameters.numberOfReaches);
            }
            Console.Write("Please enter the number of buckets (use a value of 3 for INCA) : ");
            r = Console.ReadLine();
            try
            {
                s = Convert.ToInt32(r);
                MCParameters.numberOfBoxes = s;
            }
            catch
            {
                Console.WriteLine("You did not enter a valid number, the number of buckets has been set to {0}", MCParameters.numberOfBoxes);
            }
            if (MCParameters.model == 3 || MCParameters.model==5 )  // running PECo or Tox
            {
                Console.Write("Please enter the number of sediment size classes : ");
                r = Console.ReadLine();
                try
                {
                    s = Convert.ToInt32(r);
                    MCParameters.numberOfSedimentClasses = s;
                }
                catch
                {
                    Console.WriteLine("You did not enter a valid number, the number of size classes has been set to {0}", MCParameters.numberOfSedimentClasses);
                }
            }
        }

        /// <summary>
        /// Get the number of chains to run in the MC analysis
        /// </summary>
        public static void SetRunMCParameters()
        {
            string r;
            int s;

            Console.Write("Please enter the number of parameter sets you would like in the final ensemble: ");
            r = Console.ReadLine();
            try
            {
                s = Convert.ToInt32(r);
                MCParameters.maxTries = s;
            }
            catch
            {
                Console.WriteLine("You did not enter a valid number, the number of parameter sets has been set to {0}", MCParameters.maxTries);
            }

            Console.Write("Please enter the number of runs used for the identification of each candidate parameter set: ");
            r = Console.ReadLine();
            try
            {
                s = Convert.ToInt32(r);
                MCParameters.maxJumps = s;
            }
            catch
            {
                Console.WriteLine("You did not enter a valid number, the number of runs has been set to {0}", MCParameters.maxJumps);
            }
            Console.Write("Please enter the maximum number of unsuccessful jumps: ");
            r = Console.ReadLine();
            try
            {
                s = Convert.ToInt32(r);
                MCParameters.maxUnsuccessfulJumps = s;
            }
            catch
            {
                Console.WriteLine("You did not enter a valid number, the number of unsuccessful jumps has been set to {0}", MCParameters.maxUnsuccessfulJumps);
            }
            Console.Write("Please enter the number of files to use for model output: ");
            r = Console.ReadLine();
            try
            {
                s = Convert.ToInt32(r);
                MCParameters.splitsToUse = s;
            }
            catch
            {
                Console.WriteLine("You did not enter a valid number, the number of files for model output has been set to {0}", MCParameters.splitsToUse);
            }
        }

        public static void addAvailableModels()
        {
            availableModels = new List<ModelStruct>();
            availableModels.Add(new ModelStruct(1, "PERSiST_1.4", "1.4.x"));
            availableModels.Add(new ModelStruct(2, "INCA-C", "1.7"));
            availableModels.Add(new ModelStruct(3, "INCA-PEco", "All"));
            availableModels.Add(new ModelStruct(4, "INCA-P", "1.4.x"));
            availableModels.Add(new ModelStruct(5, "INCA-Contaminants (Including MP)", "All"));
            availableModels.Add(new ModelStruct(6, "INCA-Path", "1.0"));
            availableModels.Add(new ModelStruct(7, "INCA-Hg", "1.4"));
            availableModels.Add(new ModelStruct(8, "PERSiST_1.6", "1.6.x"));
            availableModels.Add(new ModelStruct(9, "INCA(ON)THE", "1.0"));
            availableModels.Add(new ModelStruct(10, "PERSiST_2", "2.0"));
            availableModels.Add(new ModelStruct(11, "INCA_C_v2", "2.0"));
            availableModels.Add(new ModelStruct(12, "INCA_N_v1", "1.0"));
        }
    }
}
