using System;
using System.Collections.Generic;
using System.Collections;
using System.Linq;
using System.Text;
using System.IO;

namespace MC
{
    /// <summary>
    /// parameters to be passed in to create an INCA command string
    /// </summary>
    public class CommandParameter
    {
        public string name;
        public string code;
        public string value;

        /// <summary>
        /// initializer for a command parameter
        /// </summary>
        /// <param name="n">parameter name</param>
        /// <param name="c">parameter code</param>
        public CommandParameter(string n, string c)
        {
            name = n;
            code = c;
        }

        /// <summary>
        /// prepare text to add to INCA command string based on new parameter input
        /// </summary>
        /// <returns>formatted text to append to INCA command string</returns>
        public string TextToAppendToCommandLine()
        {
            string appendString = "";

            //only return something when the command argument has a value
            if (value.Length > 0) { appendString = " " + code + " " + value; }

            return appendString;
        }
    }

    /// <summary>
    /// create model version specific command strings
    /// </summary>
    public class CommandString
    {
        protected string modelExecutable;
        protected ArrayList commandArguments;
        public string commandLine;

        /// <summary>
        /// add command line parameters specific to PERSIST
        /// </summary>
        private void DoPERSiSTStuff()
        {
            //this needs to be linked to the output in Summarize Results - right now they are not connected
            commandArguments.Add(new CommandParameter("INCA output file", "-INCA"));
        }

        /// <summary>
        /// add command line parameters specific to INCA-C
        /// </summary>
        private void DoINCA_CStuff()
        {
                    commandArguments.Add(new CommandParameter("Organic Fertiliser Time Series File", "-of"));
                    commandArguments.Add(new CommandParameter("Terrestrial PDC inputs", "-tdpc"));
                    commandArguments.Add(new CommandParameter("Aquatic PDC inputs", "-adpc"));
                    commandArguments.Add(new CommandParameter("Land Use Periods", "-lup"));
                    commandArguments.Add(new CommandParameter("Deposition Time Series File", "-dep"));
                    commandArguments.Add(new CommandParameter("Spatial Time Series", "-spatial"));
        }

        /// <summary>
        /// add command line parameters specific to INCA-Pathogens
        /// </summary>
        private void DoINCAPathStuff()
        {
            //may not need any additional command line arguments
        }

        /// <summary>
        /// add command line parameters specific to "classic" INCA-N
        /// </summary>
        private void DoINCANStuff()
        {
            //add deposition time series
            commandArguments.Add(new CommandParameter("Deposition Time Series File", "-dep"));
        }

        /// <summary>
        /// add command line parameters specific to INCA-Hg
        /// </summary>
        private void DoINCAHgStuff()
        {
            //may not need any additional command line arguments
        }

        /// <summary>
        /// add command line parameters specific to INCA-ON Terrestrial Hydrology Edition
        /// </summary>
        private void DoINCAONTHEStuff()
        {
            //add deposition time series
            commandArguments.Add(new CommandParameter("Deposition Time Series File", "-dep"));
        }
        /// <summary>
        /// add command line parameters specific to INCA-PEco
        /// </summary>
        private void DoINCAPEcoStuff()
        {
            DoINCAPStuff();
        }

        /// <summary>
        /// add command line parameters specific to INCA-P
        /// </summary>
        private void DoINCAPStuff()
        {
            commandArguments.Add(new CommandParameter("Precip Type", "-precip"));           // Select which precipitation to use for infiltration calculation, defaults to "AP"
            //commandArguments.Add(new commandParameter("Snow AP (must enter -snowAP)"));   //a bit messy but gets snowAP in
            commandArguments.Add(new CommandParameter("Snow correction", "-snowAP"));
            commandArguments.Add(new CommandParameter("Grouping", "-grouping"));	        // Select how results files are grouped but branched systems, defaults to "all"
            commandArguments.Add(new CommandParameter("Solid Fertiliser File", "-sfert"));	// Solid fertiliser time series filename        
            commandArguments.Add(new CommandParameter("Liquid Fertiliser File", "-lfert"));	// Liquid fertiliser time series filename
            //commandArguments.Add(new commandParameter("Effluent", "-eff"));			    // Effluent time series filename
            //commandArguments.Add(new commandParameter("Abstraction", "-abs"));			// Abstraction time series filename
            commandArguments.Add(new CommandParameter("Growth Periods File", "-grow"));		// Growth period time series filename
            commandArguments.Add(new CommandParameter("Land Use Periods","-land"));			// Land use period time series filename
            //commandArguments.Add(new commandParameter("Structure File","-structure"));	// Structure file filename
            commandArguments.Add(new CommandParameter("Multi-branch Spatial File","-spatial"));			// Spatially distributed time series filename (not needed for main stem-only applications)
            //commandArguments.Add(new commandParameter("Observed","-obs"));			    // Observed data filename. Writes goodness-of-fit coefficients to a file called 'coefficients.csv'
            commandArguments.Add(new CommandParameter("Log file","-log"));			        // Write an error log to <filename>
            commandArguments.Add(new CommandParameter("Reference Run Number","-ref"));		// Reference run number, written to error log
            commandArguments.Add(new CommandParameter("Error File","-errfile"));	        // Select what to do with solver warnings:
        }

        /// <summary>
        /// add parameters specific to INCA-Tox (including early versions of INCA-Microplastics)
        /// </summary>
        private void DoINCAToxStuff()
        {
            commandArguments.Add(new CommandParameter("Organic Fertiliser Time Series","-ofert"));			// Organic fertiliser time series filename
            commandArguments.Add(new CommandParameter("Terrestrial PDC Time Series","-opdc"));			// Organic (terrestrial) PDC time series filename
            commandArguments.Add(new CommandParameter("Aquatic PDC Time Series","-apdc"));			// Aquatic PDC time series filename
            commandArguments.Add(new CommandParameter("Contaminant Wet Deposition Time Series","-toxwetdep"));			// Contaminant wet deposition time series filename
            commandArguments.Add(new CommandParameter("Contaminant Dry Deposition Time Series","-toxdrydep"));			// Contaminant dry deposition time series filename
            commandArguments.Add(new CommandParameter("Contaminant Litterfall Time Series","-toxlitter"));			// Contaminant litter fall time series filename
            commandArguments.Add(new CommandParameter("Contaminant Fertiliser Time Series","-toxfert"));			// Contaminant fertilizer time series filename
            commandArguments.Add(new CommandParameter("Contaminant Atmospheric Concentration Time Series","-toxconc"));			// Contaminant atmospheric concentration time series filename
            commandArguments.Add(new CommandParameter("Growth Periods","-grow"));			// Growth period time series filename
            commandArguments.Add(new CommandParameter("Land Use Periods","-land"));			// Land use period time series filename
            commandArguments.Add(new CommandParameter("Spatial Time Series","-spatial"));			// Spatially distributed time series filename (not needed for main stem-only applications)
            InteractWithModel.SetNumberOfContaminants();
       }

        /// <summary>
        /// generate a model specific command string
        /// </summary>
        public CommandString()
        {
            commandArguments = new ArrayList
            {
                new CommandParameter("Parameter File", "-par"),
                new CommandParameter("Data File", "-dat"),
                new CommandParameter("Observed Data File", "-obs"),
                new CommandParameter("Abstraction Time Series File", "-abs"),
                new CommandParameter("Effluent Time Series File", "-eff"),
                new CommandParameter("System structure file", "-structure")
            };


            switch (MCParameters.model)
            {
                case 1:
                case 8:
                case 10:
                    modelExecutable = "persist_cmd.exe -numbered ";
                    DoPERSiSTStuff();
                    break;
                case 2:
                case 11:
                    modelExecutable = "inca_c_cmd.exe";
                    DoINCA_CStuff();
                    break;
                case 3:
                    modelExecutable = "inca_pEco_cmd.exe -writeCoefficients";
                    DoINCAPEcoStuff();
                    break;
                case 4:
                    modelExecutable = "inca_p_cmd.exe -writeCoefficients ";
                    DoINCAPStuff();
                    break;
                case 5:
                    modelExecutable = "inca_contaminants_cmd.exe -writeCoefficients -snowAP -missing 0.0 ";
                    DoINCAToxStuff();
                    break;
                case 6:
                    modelExecutable = "inca_path_cmd.exe -writeCoefficients ";
                    DoINCAPathStuff();
                    break;
                case 7:
                    modelExecutable = "inca_hg_cmd.exe -writecoefficients ";
                    DoINCAHgStuff();
                    break;
                case 9:
                    modelExecutable = "inca_on_the_cmd.exe -COUP -snow -writecoefficients ";
                    DoINCAONTHEStuff();
                    break;
                case 12: 
                    modelExecutable = "inca_n_cmd.exe -writecoefficients -precip HER ";
                    DoINCANStuff();
                    break;
                default:
                   Console.WriteLine("This should not happen! Unrecognised model in Command string");
                   break;
            }
        }

        /// <summary>
        /// write the command line
        /// </summary>
        void Write()
        {
            commandLine = modelExecutable;

            foreach (CommandParameter c in commandArguments)
            {
                commandLine += c.TextToAppendToCommandLine();
            }
        }

        /// <summary>
        /// get values to populate a command string
        /// </summary>
        public void Populate()
        {
            string tmpValue;

            foreach (CommandParameter p in commandArguments)
            {
                //fix so that the appropriate parameter file name is always used in the simulations, 
                //regardless of what the user enters
                Console.Write("Please enter the name of the {0} : ", p.name);
                tmpValue = Console.ReadLine();
                //can assign tmpValue to p.value for all cases except parameter file name
                if (p.code.Equals("-par"))
                    p.value= SetParFileName(tmpValue);
                else
                    p.value = tmpValue;
            }
            this.Write();
        }

        /// <summary>
        /// set the parameter file name
        /// </summary>
        /// <param name="candidateParFileName">almost always "mc.par"</param>
        /// <returns>string containing the parameter file name</returns>
        private String SetParFileName(string candidateParFileName)
        {
            //see if something other than the default parameter file name has been entered
            if(!(candidateParFileName.Equals(MCParameters.MCParFile,StringComparison.OrdinalIgnoreCase)))
            {
                try   { File.Copy(candidateParFileName, MCParameters.MCParFile, true); }
                catch { Console.WriteLine("Problems indentifying .par file"); }
            }
            return MCParameters.MCParFile;
        }
    }
}
