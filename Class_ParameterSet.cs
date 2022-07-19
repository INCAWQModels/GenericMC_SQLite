using System;
using System.Collections;
using System.IO;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace MC
{
    public class parameterSet : ArrayList
    {
        protected ArrayList line;
        string name;

        public double modelPerformance;

        public parameterSet()
            : base()        { }

        public parameterSet(string fileName)
        {
            {
                this.name = fileName;
                foreach (string stringLine in File.ReadLines(fileName))
                {
                    line = new ArrayList();
                    string trimLine = stringLine.Trim();
                    string[] values = trimLine.Split();

                    foreach (string value in values)
                    {
                        try { line.Add(new parameterDouble(Convert.ToDouble(value), 0, 0)); }
                        catch { line.Add(new parameterString(value)); }
                    }
                    this.Add(line);
                }
            }
        }

        public parameterSet Copy()
        {
            parameterSet p = new parameterSet();

            p.name = this.name + " Copy";

            foreach (ArrayList lSource in this)
            {
                //create a parameterDouble to get the appropriate return type for a double
                parameterDouble testPar = new parameterDouble(-1,0,0);

                ArrayList pList = new ArrayList();
                foreach (parameter pSource in lSource)
                {
                    if (pSource.GetType() == testPar.GetType())
                    {
                        parameterDouble d = ((parameterDouble)pSource).Copy();
                        pList.Add(d);
                    }
                    else // assume the parameter is a string
                    {
                        parameterString s = ((parameterString)pSource).Copy();
                        pList.Add(s);
                    }
                }
                p.Add(pList);
            }
            return p;
        }

        public parameterSet(string fileName, string minFileName, string maxFileName)
        {
            try {
                StreamReader parFile = new StreamReader(fileName);
                StreamReader minFile = new StreamReader(minFileName);
                StreamReader maxFile = new StreamReader(maxFileName);

                string parFileLine;
                while ((parFileLine = parFile.ReadLine()) != null)
                {
                    //string parFileLine = parfile.ReadLine();
                    string minFileLine = minFile.ReadLine();
                    string maxFileLine = maxFile.ReadLine();

                    string trimLine = parFileLine.Trim();
                    string[] values = trimLine.Split();

                    trimLine = minFileLine.Trim();
                    string[] minValues = trimLine.Split();

                    trimLine = maxFileLine.Trim();
                    string[] maxValues = trimLine.Split();

                    line = new ArrayList();
                    for (int i = 0; i < values.Length; i++)
                    {
                        try { line.Add(new parameterDouble(Convert.ToDouble(values[i]), Convert.ToDouble(minValues[i]), Convert.ToDouble(maxValues[i]))); }
                        catch { line.Add(new parameterString(values[i])); }
                    }
                    this.Add(line);
                }
                parFile.Close();
                minFile.Close();
                maxFile.Close();
            }
            catch (Exception ex)
            {Console.WriteLine(ex.Message); };
        }

        public int write(string fileName)
        {
            if (File.Exists(fileName))
                File.Delete(fileName);

            try
            {
                TextWriter outputParFile = new StreamWriter(fileName);
                foreach (ArrayList l in this)
                {
                     int q = 1;
                    foreach (parameter p in l)
                    {
                        if(q.Equals(l.Count))
                            outputParFile.WriteLine("{0}",p.stringValue());
                        else
                            outputParFile.Write("{0} ", p.stringValue());
                        q++;
                    }

                }
                outputParFile.Close();
                return 0;
            }
            catch
            {
                return -1;
            }
        }


        public int write(string fileName, string SMListFileName)
            //special version of write adapted for modifying square matrix values in PERSiST
        {
            if(!File.Exists(SMListFileName))
            {
                Console.WriteLine("Missing list of square matrix values");
                return -1;
            }

            if (File.Exists(fileName))
                File.Delete(fileName);

            try
            {
                TextWriter outputParFile = new StreamWriter(fileName);
                foreach (ArrayList l in this)
                {
                    int q = 1;
                    foreach (parameter p in l)
                    {
                        if (q.Equals(l.Count))
                            outputParFile.WriteLine("{0}", p.stringValue());
                        else
                            outputParFile.Write("{0} ", p.stringValue());
                        q++;
                    }

                }
                outputParFile.Close();
                return 0;
            }
            catch
            {
                return -1;
            }
        }

        public int writeToArray(string rowID, string fileName)
        {
            StringBuilder appendString = new StringBuilder(rowID);

            foreach (ArrayList l in this)
            {
                foreach (parameter p in l)
                    appendString.Append(MCParameters.separatorChar + " " + p.stringValue());
            }

            StreamWriter sw;
            if (!File.Exists(fileName))
                sw = File.CreateText(fileName);
            else
                sw = File.AppendText(fileName);

            sw.WriteLine(appendString.ToString());
            sw.Close();
            return 0;
        }

        public int updateFromArray(string rowID, string fileName)
        {
            if (!File.Exists(fileName))
            {
                return -1;
            }
            else
            {
                using (StreamReader sr = new StreamReader(fileName))
                {
                    string fileLine;
                    while ((fileLine = sr.ReadLine()) != null)
                    {
                        string[] values = fileLine.Split(MCParameters.separatorChar);
                        //there is probably a more elegant way of searching a file
                        if (rowID.Equals(values[0], StringComparison.Ordinal))
                        {
                            int i = 1;

                            foreach (ArrayList psLine in this)
                            {
                                foreach (parameter ps in psLine)
                                {
                                        ps.update(values[i++]);
                                }
                            }
                            break;
                        }
                    }
                }
                return 0;
            }
        }

        public void LHS_Sample(int Offset, LHSController LHS)
        {
            for(int i=0; i<LHS.Count; i++)
            {
                ArrayList paramArray = (ArrayList)this[i];
                ArrayList LHSArray = (ArrayList)LHS[i];

                for (int j=0; j<paramArray.Count; j++)
                {
                    parameter p = (parameter)paramArray[j];
                    int[] q = (int[])LHSArray[j];
                    p.jump(q[Offset], LHS.Divisions);
                }
            }
        }

        public void MCMC_Sample(updateType u, MCMCController MCMC)
        {
            if (u == updateType.replace)
                GLUEAccounting.saveForGLUE = true;

            for (int i = 0; i < this.Count; i++)
            {
                ArrayList paramArray = (ArrayList)this[i];
                double[] JumpArray = (double[])MCMC[i];

                for (int j = 0; j < paramArray.Count; j++)
                {
                    parameter p = (parameter)paramArray[j];
                    double candidateMCMCjump= JumpArray[j];
                    //This could be the place to insert logic for the square matrix
                    p.MCMCupdate(u, candidateMCMCjump);
                }
            }
        }
        
        public void randomizeValues()
        {
            //set the GLUE flag as this is a candidate to save
            GLUEAccounting.saveForGLUE = true;

            foreach (ArrayList p in this)
            {
                foreach (parameter d in p) { d.jump(0,1); }
            }
        }

        public int writeToFileAsList(int runNumber, string fileName)
        {
             StreamWriter sw;
            if (!File.Exists(fileName))
                sw = File.CreateText(fileName);
            else
                sw = File.AppendText(fileName);

            int m = 0;
            foreach (ArrayList l in this)
            {
                //need to separate out comma-separated lists of land use types
                foreach (parameter p in l)
                {
                    string[] s = (p.stringValue()).Split(MCParameters.separatorChar);
                    foreach (string par in s)
                    {
                        StringBuilder appendString = new StringBuilder();
                        appendString.Append(runNumber.ToString() + MCParameters.separatorChar);
                        appendString.Append((m++).ToString() + MCParameters.separatorChar);
                        appendString.Append(par);
                        sw.WriteLine(appendString.ToString());
                    }
                }
            }
            sw.Close();
            return 1;
        }
    }
}
