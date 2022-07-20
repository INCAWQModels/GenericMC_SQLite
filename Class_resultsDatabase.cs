﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using System.Data.SQLite;
using System.Collections;
using System.IO;

namespace MC
{
    class resultsDatabase
    {
        protected SQLiteConnection localConnection;

        protected string cs;

        public resultsDatabase()
        {
            cs = "URI=file:" + Directory.GetCurrentDirectory() + "\\mc.db";
            localConnection = new SQLiteConnection(cs);

            cleanUp();
            makeBaseTables();
            //makeViews();
            makeCoefficientsTable();
            makeResultsTable();
        }

        /// <summary>
        /// remove old tables in the database
        /// </summary>
        private void cleanUp()
        {
            try { localConnection.Open(); }
            catch (SQLiteException ex) {Console.WriteLine(ex.Message); }
            //only try to clean up if the connection is open
            if (localConnection.State == ConnectionState.Open)
            {
                executeSQLCommand("DROP TABLE IF EXISTS ParNames");
                executeSQLCommand("DROP TABLE IF EXISTS ParList");
                executeSQLCommand("DROP TABLE IF EXISTS SortedParameters");
                executeSQLCommand("DROP TABLE IF EXISTS CoefficientWeights");
                executeSQLCommand("DROP TABLE IF EXISTS Coefficients");
                executeSQLCommand("DROP TABLE IF EXISTS Results");
                executeSQLCommand("DROP TABLE IF EXISTS IncaInputs");
                executeSQLCommand("DROP TABLE IF EXISTS Observations");
                executeSQLCommand("DROP TABLE IF EXISTS ParameterSensitivitySummary");
                executeSQLCommand("DROP TABLE IF EXISTS AllParRanges");
                executeSQLCommand("DROP TABLE IF EXISTS SampledParRanges");
                executeSQLCommand("DROP TABLE IF EXISTS ParameterIDRanges");
                localConnection.Close();
            }
            else { Console.WriteLine("Could not clean up database"); };
        }

        private void makeBaseTables()
        {
            try { localConnection.Open(); }
            catch (SQLiteException ex) { Console.WriteLine(ex.Message); }
            //only try to clean up if the connection is open
            if (localConnection.State == ConnectionState.Open)
            {
                createParNamesTable();
                createParListTable();
                createSortedParametersTable();
                createCoefficientWeightsTable();
                localConnection.Close();
            }
        }
        private void createParNamesTable()
        {
            //assume we only get here if the connection state is open
            using var cmd = new SQLiteCommand(localConnection)
            {
                CommandText = "CREATE TABLE IF NOT EXISTS ParNames ( " +
                "ParID      INTEGER NOT NULL, " +
                "ParName    TEXT)"
            };
            cmd.ExecuteNonQuery();
        }
        private void createParListTable()
        {
            using var cmd = new SQLiteCommand(localConnection)
            {
                CommandText = "CREATE TABLE IF NOT EXISTS ParList ( " +
                "RunID          INTEGER NOT NULL, " +
                "ParID          INTEGER NOT NULL, " +
                "TextValue      TEXT, " +
                "NumericValue   REAL)"
            };
            cmd.ExecuteNonQuery();
        }
        private void createSortedParametersTable()
        {
            using var cmd = new SQLiteCommand(localConnection)
            {
                CommandText = "CREATE TABLE IF NOT EXISTS SortedParameters ( " +
                "ID             INTEGER PRIMARY KEY AUTOINCREMENT, " +
                "ParID          INTEGER NOT NULL, " +
                "ParameterValue REAL, " +
                "RunID          INTEGER)"
            };
            cmd.ExecuteNonQuery();
        }

        private void createCoefficientWeightsTable()
        {
            //assume we only get here if the connection state is open
            using var cmd = new SQLiteCommand(localConnection)
            {
                CommandText = "CREATE TABLE IF NOT EXISTS CoefficientWeights ( " +
                "CoefficientName    TEXT NOT NULL, " +
                "CoefficientWeight  REAL NOT NULL)"
            };
            cmd.ExecuteNonQuery();
        }
        /// <summary>
        /// create the views needed for the queries to run
        /// </summary>
        public void makeViews()
        {
            try { localConnection.Open(); }
            catch (SQLiteException ex) { Console.WriteLine(ex.Message); }
            //only try to clean up if the connection is open
            if (localConnection.State == ConnectionState.Open)
            {
                createTableAllParRanges();
                createTableSampledParsRanges();
                localConnection.Close();
            }
        }

        private void createTableAllParRanges()
        {
            using var testCmd = new SQLiteCommand(localConnection)
            {
                CommandText = "SELECT COUNT(*) FROM ParList"
            };
            string recs = testCmd.ExecuteScalar().ToString();
            Console.WriteLine($"Number of records : {recs}");

            using var cmd = new SQLiteCommand(localConnection)
            {
                CommandText = "CREATE TABLE IF NOT EXISTS AllParRanges AS " +
                "SELECT ParList.ParID, MIN(ParList.NumericValue) AS MinOfNumericValue, " +
                "AVG(ParList.NumericValue) As AvgOfNumericValue, " +
                "MAX(ParList.NumericValue) As MaxOfNumericValue " +
                "FROM ParList GROUP BY ParList.ParID"
            };
            cmd.ExecuteNonQuery();
        }
        private void createTableSampledParsRanges()
        {
            using var cmd = new SQLiteCommand(localConnection)
            {
                CommandText = "CREATE TABLE IF NOT EXISTS SampledParRanges AS " +
                "SELECT ParID, " +
                "MinOfNumericValue, " +
                "AvgOfNumericValue, " +
                "MaxOfNumericValue " +
                "FROM AllParRanges " +
                "WHERE MinOfNumericValue <> MaxOfNumericValue"
            };
            cmd.ExecuteNonQuery();
        }
        
        internal void createTableParameterIDRanges()
        {
            try { localConnection.Open(); }
            catch (SQLiteException ex) { Console.WriteLine(ex.Message); }
            //only try to clean up if the connection is open
            if (localConnection.State == ConnectionState.Open)
            {
                executeSQLCommand("CREATE TABLE IF NOT EXISTS ParameterIDRanges AS " +
                    "SELECT ParID, " +
                    "MIN(ID) As MinOfID, " +
                    "MAX(ID) As MaxOfID, " +
                    "MIN(ParameterValue) As MinOfParameterValue, " +
                    "MAX(ParameterValue) As MaxOfParameterValue " +
                    "FROM SortedParameters " +
                    "GROUP BY ParID"
                );
                localConnection.Close();
            }
            else
            {
                Console.WriteLine("Could not create ParameterRangeID table");
                Console.ReadLine();
            };
        }
        internal void processParameterData()
        {
            /*
            using var cmd = new SQLiteCommand(localConnection)
            {
                CommandText = "INSERT INTO SortedParameters ( ParID, ParameterValue, RunID )" +
                    "SELECT ParList.ParID, ParList.NumericValue, ParList.RunID " +
                    "FROM ParList INNER JOIN SampledParRanges ON ParList.ParID = SampledParRanges.ParID " +
                    "ORDER BY ParList.ParID, ParList.NumericValue"
            };
            cmd.ExecuteNonQuery();
            */
          
            try { localConnection.Open(); }
            catch (SQLiteException ex) { Console.WriteLine(ex.Message); }
            //only try to clean up if the connection is open
            if (localConnection.State == ConnectionState.Open)
            {
                executeSQLCommand("INSERT INTO SortedParameters ( ParID, ParameterValue, RunID )" +
                    "SELECT ParList.ParID, ParList.NumericValue, ParList.RunID " +
                    "FROM ParList INNER JOIN SampledParRanges ON ParList.ParID = SampledParRanges.ParID " +
                    "ORDER BY ParList.ParID, ParList.NumericValue"
                );
                localConnection.Close();
            }
            else { 
                Console.WriteLine("Could not fix parameters");
                Console.ReadLine();
            };
        }

       
        internal void createParameterSensitivitySummaryTable()
        {
            try { localConnection.Open(); }
            catch (SQLiteException ex) { Console.WriteLine(ex.Message); }

            string SQLstring = "SELECT[116 Statistics Summary].ParName, [116 Statistics Summary].ParID, [116 Statistics Summary].D, " +
                " [116 Statistics Summary].MinOfNumericValue, [116 Statistics Summary].MaxOfNumericValue, [116 Statistics Summary].xRange, " +
                " [116 Statistics Summary].z, [116 Statistics Summary].p INTO ParameterSensitivitySummary FROM [116 Statistics Summary]";
            executeSQLCommand(SQLstring);
            localConnection.Close();
        }

        private void notYetImplemented()
        {
            //write a message to let the user know this feature does not yet exist for this verions of INCA/PERSiST
            Console.WriteLine("This feature is not yet implemented for this verion of INCA");
            Console.WriteLine("Text files are generated which can be used for subsequent analysis");
        }

        private void makeResultsTable()
        {
            try { localConnection.Open(); }
            catch (SQLiteException ex) { Console.WriteLine(ex.Message); }
            switch (MCParameters.model)
            {
                case 1: //PERSiST 1.4
                case 8: // PERSiST 1.6
                case 10: //PERSiST v2
                    makePERSiSTResultsTable();
                    makeINCAInputsTable();
                    break;
                case 2: //INCA-C
                    makeINCA_CResultsTable();
                    break;
                case 3: //INCA-PEco
                case 4: //INCA-P
                case 5: //INCA-Contaminants
                case 6: //INCA-Path
                case 11: //INCA-C v2.x
                case 12: //INCA-N Classic
                    notYetImplemented();
                    break;
                case 7:
                    makeINCA_HgResultsTable();
                    break;
                case 9:
                    makeINCA_ONTHEResultsTable();
                    break;
                default:
                    Console.Write("Something has gone wrong when making the COEFFICIENTS table");
                    break;
            }
            localConnection.Close();
        }

        private void makePERSiSTResultsTable()
        {
            string SQLString;

            SQLString = "CREATE TABLE Results (" +
                "RUN            INTEGER," +
                "RowNumber      INTEGER," +
                "Reach          TEXT(255)," +
                "TerrestrialInput   DOUBLE," +
                "Flow           DOUBLE," +
                "DateStamp      DATE)";
            executeSQLCommand(SQLString);
        }

        private void makeINCA_CResultsTable()
        {
            string SQLString;

            SQLString = "CREATE TABLE Results (" +
                "RUN                INTEGER," +
                "RowNumber          INTEGER," +
                "Reach              TEXT(255)," +
                "Flow               DOUBLE," +
                "DateStamp          DATE)";
            executeSQLCommand(SQLString);
        }

        private void makeINCA_HgResultsTable()  // needs to be cleaned up for INCA_Hg outputs
        {
            string SQLString;

            SQLString = "CREATE TABLE Results (" +
                "RUN        INTEGER," +
                "RowNumber  INTEGER," +
                "Reach      TEXT(255)," +
                "Flow       DOUBLE," +
                "DateStamp  DATE)";
            executeSQLCommand(SQLString);
        }

        private void makeINCAInputsTable()
        {
            string SQLString;

            SQLString = "CREATE TABLE INCAInputs (" +
                "FileName       TEXT(255)," +
                "RUN            INTEGER," +
                "RowNumber      INTEGER,"+
                "SMD            DOUBLE," +
                "HER            DOUBLE," +
                "T              DOUBLE," +
                "P              DOUBLE," +
                "DateStamp      DATE)";
            executeSQLCommand(SQLString);
        }

        private void makeINCA_ONTHEResultsTable()
        {
            string SQLString;

            SQLString = "CREATE TABLE Results (" +
                "FileName       TEXT(255)," +
                "RUN            INTEGER," +
                "RowNumber      INTEGER," +
                "FLOW           DOUBLE," +
                "NITRATE        DOUBLE," +
                "AMMONIUM       DOUBLE," +
                "VOLUME         DOUBLE," +
                "DON            DOUBLE," +
                "VELOCITY       DOUBLE," +
                "WIDTH          DOUBLE," +
                "DEPTH          DOUBLE," +
                "AREA           DOUBLE," +
                "PERIMETER      DOUBLE," +
                "RADIUS         DOUBLE," +
                "RESIDENCETIME  DOUBLE," +
                "DateStamp      DATE)";
            executeSQLCommand(SQLString);
        }

        private void makeINCA_PResultsTable()
        {
            string SQLString;

            SQLString = "CREATE TABLE Results (" +
                "FileName           TEXT(255), " +
                "RUN                INTEGER," +
                "RowNumber          INTEGER," +
                "Discharge          DOUBLE, " +
                "Volume             DOUBLE, " +
                "Velocity           DOUBLE, " +
                "WaterDepth         DOUBLE, " +
                "StreamPower        DOUBLE, " +
                "ShearVelocity      DOUBLE, " +
                "MaxEntGrainSize    DOUBLE, " +
                "MoveableBedMass    DOUBLE, " +
                "EntrainmentRate    DOUBLE, " +
                "DepositionRate     DOUBLE, " +
                "BedSediment        DOUBLE, " +
                "SuspendedSediment  DOUBLE, " +
                "DiffuseSediment    DOUBLE, " +
                "WaterCOlumnTDP     DOUBLE, " +
                "WaterColumnPP      DOUBLE," +
                "WCSorptionRelease  DOUBLE, " +
                "StreamBedTDP       DOUBLE, " +
                "StreamBedPP        DOUBLE, " +
                "BedSorptionRelease DOUBLE, " +
                "MacrophyteMass     DOUBLE, " +
                "EpiphyteMass       DOUBLE, " +
                "WaterColumnTP      DOUBLE, " +
                "WaterColumnSRP     DOUBLE, " +
                "WaterTemperature   DOUBLE, " +
                "TDPInput           DOUBLE, " +
                "PPInput            DOUBLE, " +
                "WaterColumnEPC0    DOUBLE, " +
                "StreamBedEPC0      DOUBLE, " +
                "DateStamp          Date)";
            executeSQLCommand(SQLString);
        }

        public void makeObservationsTable()
        {
            //we have the same observations table for each version of the model so do not have to do anything special
            try { localConnection.Open(); }
            catch (SQLiteException ex) { Console.WriteLine(ex.Message); }
            string SQLString = "CREATE TABLE Observations (Reach  TEXT(255)," +
                "Parameter  TEXT(255)," +
                "Value      DOUBLE," +
                "QC         TEXT(255)" +
                "DateStamp  DATE)";
            executeSQLCommand(SQLString);
            localConnection.Close();
        }

        private void makeCoefficientsTable()
        {
            try { localConnection.Open(); }
            catch (SQLiteException ex) { Console.WriteLine(ex.Message); }
            switch (MCParameters.model)
            {
                case 1: //PERSiST 1.4
                case 8: // PERSiST 1.6
                    makePERSiSTCoefficientsTable();
                    break;
                case 2: //INCA-C
                case 11: //INCA-C 2.x
                    makeINCA_CCoefficientsTable();
                    break;
                case 3: //INCA-PEco
                    makeINCA_PEcoCoefficientsTable();
                    break;
                case 4: //INCA-P
                    makeINCA_PCoefficientsTable();
                    break;
                case 5: //INCA-Contaminants
                    notYetImplemented();
                    break;
                case 6: //INCA-Path
                    notYetImplemented();
                    break;
                case 7:
                    makeINCA_HgCoefficientsTable();
                    break;
                case 9:
                    makeINCA_ONTHECoefficientsTable();
                    break;
                case 10: //PERSiST v2
                    makePERSiST_v2CoefficientsTable();
                    break;
                case 12: //INCA-N
                    makeINCA_NCoefficientsTable();
                    break;
                default:
                    Console.Write("Something has gone wrong when making the COEFFICIENTS table");
                    Console.ReadLine();
                    break;
            }
            localConnection.Close();
        }

        private void makeDefaultCoefficientsTable()
        {
            string SQLString;

            SQLString = "CREATE TABLE Coefficients (" +
                "RUN        INTEGER," +
                "RowNumber  INTEGER," +
                "Reach      TEXT(255)," +
                "Parameter  TEXT(255)," +
                "R2         DOUBLE," +
                "NS         DOUBLE," +
                "RMSE       DOUBLE," +
                "RE         DOUBLE," +
                "DateStamp  DATE)";
            executeSQLCommand(SQLString);
        }

        private void makeINCA_NCoefficientsTable()
        {
            makeDefaultCoefficientsTable();
        }

        private void makeINCA_ONTHECoefficientsTable()
        {
            string SQLString;

            SQLString = "CREATE TABLE Coefficients (" +
                "RUN        INTEGER," +
                "RowNumber  INTEGER," +
                "Reach      TEXT(255)," +
                "Parameter  TEXT(255)," +
                "R2         DOUBLE," +
                "NS         DOUBLE," +
                "logNS      DOUBLE," +
                "AD         DOUBLE," +
                "VAR        DOUBLE," +
                "KGE        DOUBLE," +
                "DateStamp  DATE)";
            executeSQLCommand(SQLString);
        }

        private void makeINCA_PEcoCoefficientsTable()
        {
            string SQLString;

            SQLString = "CREATE TABLE Coefficients (" +
                "RUN        INTEGER," +
                "RowNumber  INTEGER," +
                "Reach      TEXT(255)," +
                "Parameter  TEXT(255)," +
                "R2         DOUBLE," +
                "NS         DOUBLE," +
                "logNS      DOUBLE, " +
                "RMSE       DOUBLE," +
                "AD         DOUBLE," +
                "VR         DOUBLE, "+
                "KGE        DOUBLE, " +
                "CAT_B      DOUBLE,"+
                "CAT_C      DOUBLE,"+   
                "CAT_CA     DOUBLE,"+
                "CAT_CB     DOUBLE,"+
                "DateStamp  DATE)";
            executeSQLCommand(SQLString);
        }
        
        private void makeINCA_PCoefficientsTable()
        {
            string SQLString;

            SQLString = "CREATE TABLE Coefficients (" +
                "RUN        INTEGER," +
                "RowNumber  INTEGER," +
                "Reach      TEXT(255)," +
                "Parameter  TEXT(255)," +
                "R2         DOUBLE," +
                "NS         DOUBLE," +
                "RMSE       DOUBLE," +
                "RE         DOUBLE," +
                "VR         DOUBLE," +
                "DateStamp  DATE)";
            executeSQLCommand(SQLString);
        }

        private void makePERSiSTCoefficientsTable()
        {
            string SQLString;

            SQLString = "CREATE TABLE Coefficients (" +
                "RUN        INTEGER," +
                "RowNumber  INTEGER," +
                "Reach      TEXT(255)," +
                "R2         DOUBLE," +
                "NS         DOUBLE," +
                "LOG_NS     DOUBLE," +
                "RMSE       DOUBLE," +
                "RE         DOUBLE," +
                "AD         DOUBLE," +
                "VAR        DOUBLE," +
                "N          DOUBLE," +
                "N_RE       DOUBLE," +
                "SS         DOUBLE," +
                "LOG_SS     DOUBLE," +
                "DateStamp  DATE)";
            executeSQLCommand(SQLString);
        }

        private void makePERSiST_v2CoefficientsTable()
        {
            string SQLString;

            SQLString = "CREATE TABLE Coefficients (" +
                "RUN        INTEGER," +
                "RowNumber  INTEGER," +
                "Reach      TEXT(255)," +
                "Parameter  TEXT(255)," +
                "R2         DOUBLE," +
                "NS         DOUBLE," +
                "LOG_NS     DOUBLE," +
                "RMSE       DOUBLE," +
                "RE         DOUBLE," +
                "AD         DOUBLE," +
                "VAR        DOUBLE," +
                "N          DOUBLE," +
                "N_RE       DOUBLE," +
                "SS         DOUBLE," +
                "LOG_SS     DOUBLE," +
                "DateStamp  DATE)";
            executeSQLCommand(SQLString);
        }
        private void makeINCA_CCoefficientsTable()
        {
            makeDefaultCoefficientsTable();
        }

        private void makeINCA_HgCoefficientsTable()
        {
            makeDefaultCoefficientsTable();
        }

        //take the summary file of results and write them all to the database (note - may want to change this later
        //so as to write one set of results at a time
        public void writeResults()
        {
            try { localConnection.Open(); }
            catch (SQLiteException ex) { Console.WriteLine(ex.Message); }
            switch (MCParameters.model)
            {
                case 1: //PERSiST 1.4
                case 8: //PERSiST 1.6
                    //writeINCAResultsFromPERSiST();
                    //writePERSiSTResults();
                    break;
                case 2: //INCA-C
                    notYetImplemented();
                    break;
                case 3: //INCA-PEco
                    notYetImplemented();
                    break;
                case 4: //INCA-P
                    notYetImplemented();
                    break;
                case 5: //INCA-Contaminants
                    notYetImplemented();
                    break;
                case 6: //INCA-Path
                    notYetImplemented();
                    break;
                case 7: //INCA-Hg
                    notYetImplemented();
                    break;
                case 9: //INCA_ONTHE
                    notYetImplemented();
                    break;
                default:
                    Console.WriteLine("Something has gone wrong when populating the RESULTS table");
                    break;
            }
            localConnection.Close();
        }

        //this is really slow, needs to be refactored before it can be useful
        private void writePERSiSTResultsToDatabase()
        {
            for (var i = 0; i < MCParameters.splitsToUse; i++)
            {
                string fileName = MCParameters.resultFileNameStub + i.ToString() + ".txt";
                using (StreamReader sr = new StreamReader(fileName))
                {
                    string line;
                    string reach="";
                    while ((line = sr.ReadLine()) != null)
                    {
                        string SQLString="INSERT INTO Results (RUN, RowNumber, Reach, TerrestrialInput, Flow, DateStamp) VALUES (";

                        string[] fields = line.Split(MCParameters.separatorChar);
                        //check if the current row is a header row or a data row (based on the number of fields)
                        if(fields.Length==3)
                        {
                            reach = fields[2];
                            //give some evidence of activity
                            Console.WriteLine("Processing results for iteration " + fields[0] + ", reach " + reach);
                        }
                        else
                        {
                            SQLString = SQLString +
                                fields[0] + ", " +        //RUN
                                fields[1] + ", '" +        //RowNumber
                                reach + "', " +          //Reach
                                fields[2] + "," +         //Diffuse Inputs from land phase
                                fields[3] + ", DATE())";
                        }

                        using (StreamWriter SQLCheck = new StreamWriter("SQLCheck.txt"))
                        {
                            SQLCheck.WriteLine(SQLString);
                        }
                        
                        SQLiteCommand tmp = new SQLiteCommand(SQLString, localConnection);
                        try { tmp.ExecuteNonQuery(); }
                        catch (Exception ex) { Console.WriteLine(ex.Message); }
                        tmp.Dispose();
                    }
                }
            }
        }

        private void writePERSiSTResults()
        {
            string PERSiSTOutputFile = "PERSiSTResults.txt";
            File.Create(PERSiSTOutputFile).Dispose();

            for (var i = 0; i < MCParameters.splitsToUse; i++)
            {
                string fileName = MCParameters.resultFileNameStub + i.ToString() + ".txt";
                using (StreamReader sr = new StreamReader(fileName))
                {
                    string line;
                    string reach = "";
                    while ((line = sr.ReadLine()) != null)
                    {
                        string[] fields = line.Split(MCParameters.separatorChar);
                        //check if the current row is a header row or a data row (based on the number of fields)
                        if (fields.Length == 3)
                        {
                            reach = fields[2];
                            //give some evidence of activity
                            Console.WriteLine("Processing results for iteration " + fields[0] + ", reach " + reach);
                        }
                        else
                        {
                            string resultString =
                                fields[0] + ", " +       //RUN
                                fields[1] + ", '" +      //RowNumber
                                reach + "', " +          //Reach
                                fields[2] + ", " +       //Diffuse Inputs from land phase
                                fields[3];               //flow
                            //check that we actually have numeric results, and if we do, write them
                            double tst;
                            try {
                                tst = Convert.ToDouble(fields[2]);
                                using (FileStream fs = new FileStream(PERSiSTOutputFile, FileMode.Append, FileAccess.Write))
                                using (StreamWriter sw = new StreamWriter(fs))
                                { sw.WriteLine(resultString); }
                                }
                            catch { }
                        }
                    }
                }
            }
        }

        //this is really slow, needs to be refactored before it can be useful
        private void writeINCAResultsFromPERSiSTToDatabase()
        {
            //assume that each candidate INCA inputs file has been generated in the current model run
            //this should work as all candidate input files are cleaned up earlier
            string[] resultFiles = Directory.GetFiles(Directory.GetCurrentDirectory(), MCParameters.INCAFileNameStub + "*.txt");
            foreach (string f in resultFiles)
            {
                using (StreamReader sr = new StreamReader(f))
                {
                    string line;
                    while ((line = sr.ReadLine()) != null)
                    {
                        string[] fields = line.Split('\t');

                        using (StreamWriter SQLCheck = new StreamWriter("SQLCheck.txt"))
                        {
                            SQLCheck.WriteLine("Fields {0}",fields.Length);
                        }

                        try
                            {
                            string SQLString = "INSERT INTO INCAInputs (FileName, RUN, RowNumber, SMD, HER, T, P, DateStamp)" +
                                "VALUES (' " + f + "', " +
                                fields[0] + "," +        //Run
                                fields[1] + "," +       //RowNumber
                                fields[2] + "," +       //SMD
                                fields[3] + "," +       //HER
                                fields[4] + "," +       //T
                                fields[5] + "," +       //P
                                "DATE())";

                            using (StreamWriter SQLCheck = new StreamWriter("SQLCheck.txt"))
                            {
                                SQLCheck.WriteLine(SQLString);
                            }

                            executeSQLCommand(SQLString);
                        }
                        catch(Exception ex)
                        {
                            Console.WriteLine(ex.Message);
                        }
                    }
                }
            }
        }

        private void writeINCAResultsFromPERSiST()
        {
            string INCASummaryFile = "INCASummary.txt";
            File.Create(INCASummaryFile).Dispose();

            //assume that each candidate INCA inputs file has been generated in the current model run
            //this should work as all candidate input files are cleaned up earlier
            string[] resultFiles = Directory.GetFiles(Directory.GetCurrentDirectory(), MCParameters.INCAFileNameStub + "*.txt");
            foreach (string f in resultFiles)
            {
                using (StreamReader sr = new StreamReader(f))
                {
                    string line;
                    string resultString;
                    while ((line = sr.ReadLine()) != null)
                    {
                        string[] fields = line.Split('\t');

                        try
                        {
                            resultString = f + ", " +
                                fields[0] + "," +        //Run
                                fields[1] + "," +       //RowNumber
                                fields[2] + "," +       //SMD
                                fields[3] + "," +       //HER
                                fields[4] + "," +       //T
                                fields[5];             //P

                            using (FileStream fs = new FileStream(INCASummaryFile, FileMode.Append, FileAccess.Write))
                            using (StreamWriter sw = new StreamWriter(fs))
                            { sw.WriteLine(resultString); }
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine(ex.Message);
                        }
                    }
                }
            }
        }

        //take the summary file of coefficients and write them all to the database (note - may want to change this later
        //so as to write one set of coefficients at a time
        public void writeCoefficients()
        {
            try { localConnection.Open(); }
            catch (SQLiteException ex) { Console.WriteLine(ex.Message); }
            switch (MCParameters.model)
            {
                case 1: //PERSiST 1.4
                case 8: // PERSiST 1.6
                    writePERSiSTCoefficients();
                    break;
                case 2:     //INCA-C v.1.x
                case 7:     // INCA-Hg
                    writeGenericINCA_Coefficients();
                    break;
                case 3: //INCA-PEco
                    writeINCA_PEcoCoefficients();
                    break;
                case 4: //INCA-P
                    notYetImplemented();
                    break;
                case 5: //INCA-Contaminants
                    notYetImplemented();
                    break;
                case 6: //INCA-Path
                    notYetImplemented();
                    break;
                case 9: //INCA_ONTHE
                    writeINCA_ONTHECoefficients();
                    break;
                case 10: //PERSIST v2
                    writePERSiST_v2Coefficients();
                    break;
                case 11: //INCA_C v.2
                    notYetImplemented();
                    break;
                case 12:    // INCA-N v.1.x
                    writeINCA_NCoefficients();
                    break;
                default:
                    Console.WriteLine("Something has gone wrong when populating the COEFFICIENTS table");
                    break;
            }
            localConnection.Close();
        }

        private void writePERSiSTCoefficients()
        {
            using (StreamReader sr = new StreamReader(MCParameters.coefficientsSummaryFile))
            {
                string line;
                while ((line = sr.ReadLine()) != null)
                {
                    {
                        try
                        {
                            string[] fields = line.Split(MCParameters.separatorChar);
                            if ((fields[1]).Equals("0")) { Console.WriteLine(line); }
                            else
                            {
                                string SQLString = "INSERT INTO Coefficients " +
                                    "(RUN, RowNumber, Reach, R2, NS, LOG_NS, RMSE, RE, AD, VAR, N, N_RE, SS, LOG_SS, DateStamp) VALUES (" +
                                    fields[0] + ", " +
                                    fields[1] + ", '" +
                                    fields[2] + "', " +
                                    fields[3] + ", " +
                                    fields[4] + "," +
                                    fields[5] + ", " +
                                    fields[6] + ", " +
                                    fields[7] + ", " +
                                    fields[8] + ", " +
                                    fields[9] + ", " +
                                    fields[10] + ", " +
                                    fields[11] + ", " +
                                    fields[12] + ", " +
                                    fields[13] + ", " +
                                    " DATE())";
                                //Console.WriteLine(SQLString);
                                using (StreamWriter SQLCheck = new StreamWriter("SQLCheck.txt"))
                                {
                                    SQLCheck.WriteLine(SQLString);
                                }
                                SQLiteCommand tmp = new SQLiteCommand(SQLString, localConnection);
                                try { tmp.ExecuteNonQuery(); }
                                catch (Exception ex) { Console.WriteLine(ex.Message); }
                                tmp.Dispose();
                            }
                        }
                        catch (Exception ex) { Console.WriteLine(ex.Message); }
                    }
                }
            }
        }

        private void writePERSiST_v2Coefficients()
        {
            using (StreamReader sr = new StreamReader(MCParameters.coefficientsSummaryFile))
            {
                string line;
                string reachName = "undefined";
                while ((line = sr.ReadLine()) != null)
                {
                    {
                        try
                        {
                            string[] fields = line.Split(MCParameters.separatorChar);
                            //
                            // get the reach name from the header row so ensure that therE are enough columns and that fields[3] is non-numeric
                            // continue to populate coefficeints if thre are enough fields
                            //
                            if (fields.Length > 3)
                            {
                                int rownum = int.Parse(fields[1]);
                                if ((rownum % 7) == 0)
                                {
                                    reachName = fields[2];
                                }
                                else
                                {
                                    string SQLString = "INSERT INTO Coefficients " +
                                        "(RUN, RowNumber, Reach, Parameter, R2, NS, LOG_NS, RMSE, RE, AD, VAR, N, N_RE, SS, LOG_SS, DateStamp) VALUES (" +
                                        fields[0] + ", " +
                                        fields[1] + ", '" +
                                        reachName + "', '" +
                                        fields[2] + "', " +
                                        fields[3] + ", " +
                                        fields[4] + "," +
                                        fields[5] + ", " +
                                        fields[6] + ", " +
                                        fields[7] + ", " +
                                        fields[8] + ", " +
                                        fields[9] + ", " +
                                        fields[10] + ", " +
                                        fields[11] + ", " +
                                        fields[12] + ", " +
                                        fields[13] + ", " +
                                        " DATE())";
                                    //Console.WriteLine(SQLString);
                                    using (StreamWriter SQLCheck = new StreamWriter("SQLCheck.txt"))
                                    {
                                        SQLCheck.WriteLine(SQLString);
                                    }
                                    SQLiteCommand tmp = new SQLiteCommand(SQLString, localConnection);
                                    try { tmp.ExecuteNonQuery(); }
                                    catch (Exception ex) { Console.WriteLine(ex.Message); }
                                    tmp.Dispose();
                                }
                            }
                        }
                        catch (Exception ex) { Console.WriteLine(ex.Message); }
                    }
                }
            }
        }
        private void writeGenericINCA_Coefficients()
        {
            using (StreamReader sr = new StreamReader(MCParameters.coefficientsSummaryFile))
            {
                string line;
                string reachName = "undefined";
                while ((line = sr.ReadLine()) != null)
                {
                    {
                        try
                        {
                            string[] fields = line.Split(MCParameters.separatorChar);
                            //
                            // get the reach name from the header row so ensure that there are enough columns and that fields[3] is non-numeric
                            // continue to populate coefficients if thre are enough fields
                            //
                            if (fields.Length > 2)
                            {
                                int rownum = int.Parse(fields[1]);
                                if ((rownum % 8) == 0)
                                {
                                    reachName = fields[2];
                                }
                                else
                                {
                                    string SQLString = "INSERT INTO Coefficients " +
                                        "(RUN, RowNumber, Reach, Parameter, R2, NS, RMSE, RE, DateStamp) VALUES (" +
                                        fields[0] + ", " +
                                        fields[1] + ", '" +
                                        reachName + "', '" +
                                        fields[2] + "', " +
                                        fields[3] + ", " +
                                        fields[4] + "," +
                                        fields[5] + ", " +
                                        fields[6] + ", " +
                                        " DATE())";
                                    //Console.WriteLine(SQLString);
                                    using (StreamWriter SQLCheck = new StreamWriter("SQLCheck.txt"))
                                    {
                                        SQLCheck.WriteLine(SQLString);
                                    }
                                    SQLiteCommand tmp = new SQLiteCommand(SQLString, localConnection);
                                    try { tmp.ExecuteNonQuery(); }
                                    catch (Exception ex) { Console.WriteLine(ex.Message); }
                                    tmp.Dispose();
                                }
                            }
                        }
                        catch (Exception ex) { Console.WriteLine(ex.Message); }
                    }
                }
            }
        }

        private void writeINCA_NCoefficients()
        {
            using (StreamReader sr = new StreamReader(MCParameters.coefficientsSummaryFile))
            {
                string line;
                string reachName = "undefined";
                while ((line = sr.ReadLine()) != null)
                {
                    {
                        try
                        {
                            string[] fields = line.Split(MCParameters.separatorChar);
                            //
                            // get the reach name from the header row so ensure that there are enough columns and that fields[3] is non-numeric
                            // continue to populate coefficients if thre are enough fields
                            //
          
                            if (fields.Length > 2)
                            {
                                int rownum = int.Parse(fields[1]);
                                if ((rownum % 6) == 0)
                                {
                                    reachName = fields[2];
                                }
                                else
                                {
                                    string SQLString = "INSERT INTO Coefficients " +
                                        "(RUN, RowNumber, Reach, Parameter, R2, NS, RMSE, RE, DateStamp) VALUES (" +
                                        fields[0] + ", " +
                                        fields[1] + ", '" +
                                        reachName + "_Reach', '" +
                                        fields[2] + "', " +
                                        fields[3] + ", " +
                                        fields[4] + "," +
                                        fields[5] + ", " +
                                        fields[6] + ", " +
                                        " DATE())";
                                    //Console.WriteLine(SQLString);
                                    using (StreamWriter SQLCheck = new StreamWriter("SQLCheck.txt"))
                                    {
                                        SQLCheck.WriteLine(SQLString);
                                    }
                                    SQLiteCommand tmp = new SQLiteCommand(SQLString, localConnection);
                                    try { tmp.ExecuteNonQuery(); }
                                    catch (Exception ex) { Console.WriteLine(ex.Message); }
                                    tmp.Dispose();
                                }
                            }
                        }
                        catch (Exception ex) { Console.WriteLine(ex.Message); }
                    }
                }
            }
        }
        private void writeINCA_ONTHECoefficients()
        {
            try
            {
                using (StreamReader sr = new StreamReader(MCParameters.coefficientsSummaryFile))
                {
                    string line;
                    string reachName = "";

                    while ((line = sr.ReadLine()) != null)
                    {
                        string[] fields = line.Split(MCParameters.separatorChar);
                        if (fields.Length == 3) //we have a reach name
                            {
                                Console.WriteLine(line);
                                reachName = fields[2];
                            }
                        else
                        {
                            if (fields.Length > 4)   //skip short lines, i.e., header and footer
                            {
                                string SQLString;

                                SQLString = "INSERT INTO Coefficients (Run, RowNumber, Reach, Parameter, R2, NS, logNS, AD, VAR, KGE, DateStamp) VALUES (" +
                                    fields[0] + ", " +
                                    fields[1] + ",'" +
                                    reachName + "', '" +
                                    fields[2] + "', " +         //parameter name
                                    fields[3] + "," +           //R2
                                    fields[4] + ", " +          //Nash Sutcliffe
                                    fields[5] + ", " +          //logNS
                                    fields[7] + ", " +          //AD
                                    fields[8] + ", " +          //Var
                                    fields[13] + ", " +           //KGE
                                    " Date())";    //RE and date


                                using (StreamWriter SQLCheck = new StreamWriter("SQLCheck.txt"))
                                {
                                    SQLCheck.WriteLine(SQLString);
                                }
                                SQLiteCommand tmp = new SQLiteCommand(SQLString, localConnection);
                                try { tmp.ExecuteNonQuery(); }
                                catch (Exception ex) {
                                    Console.WriteLine(SQLString);
                                    Console.WriteLine(ex.Message);
                                    //Console.ReadLine();
                                }
                                tmp.Dispose();
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
        }

        /* should not be necessary
        private void writeINCA_HgCoefficients()
        {
            writeDefaultCoefficients();
        }
        */

        private void writeDefaultCoefficients()
        { 
            try
            {
                using (StreamReader sr = new StreamReader(MCParameters.coefficientsSummaryFile))
                {
                    string line;
                    for (int k = 0; k < MCParameters.runsToOrganize; k++)
                    {
                        for (int i = 0; i < MCParameters.numberOfReaches; i++)
                        {
                            string reachName = "";
                            for (int j = 0; j <= 6; j++)
                            {
                                line = sr.ReadLine();
                                string[] fields = line.Split(MCParameters.separatorChar);
                                //Console.WriteLine(j.ToString() + ": " + line);
                                switch (j)
                                {
                                    case 0:
                                        {
                                            reachName = fields[2];
                                            break;
                                        }
                                    case 2:
                                    //case 3:
                                    case 4:
                                    case 5:
                                        {
                                            string SQLString;
                                            SQLString = "INSERT INTO Coefficients (Run, RowNumber, Reach, Parameter, R2, NS, RMSE, RE, DateStamp) VALUES (" +
                                                fields[0] + ", " +
                                                j.ToString() + ",'" +
                                                reachName + "', '" +
                                                fields[2] + "', " +      //parameter name
                                                fields[3] + "," +        //R2
                                                fields[4] + ", " +      //Nash Sutcliffe
                                                fields[5] + ", " +       //RMSE
                                                fields[6] + ", Date())";  //RE and date
                                            Console.WriteLine(SQLString);
                                            using (StreamWriter SQLCheck = new StreamWriter("SQLCheck.txt"))
                                            {
                                                SQLCheck.WriteLine(SQLString);
                                            }
                                            SQLiteCommand tmp = new SQLiteCommand(SQLString, localConnection);
                                            try { tmp.ExecuteNonQuery(); }
                                            catch (Exception ex) { Console.WriteLine(ex.Message); }
                                            tmp.Dispose();
                                            break;
                                        }
                                    default:
                                        break;
                                }
                            }
                        }

                    }
                }
            }
            catch(Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
        }

        private void writeINCA_PEcoCoefficients()
        {
            try
            {
                using (StreamReader sr = new StreamReader(MCParameters.coefficientsSummaryFile))
                {
                    string line;
                    string reachName = "";
                    
                    while ((line = sr.ReadLine()) != null)
                        {
                        string[] fields = line.Split(MCParameters.separatorChar);
                        // get reach names, this will happen when row number is a multiple of 16
                        //right now exceptions are not caught
                        int fieldNumber = Int32.Parse(fields[1]);
                        if ((fieldNumber % 16) == 0)
                        {
                            Console.WriteLine(line);
                            reachName = fields[2];
                        }
                        if (fields.Length>10)   //skip short lines, i.e., header and footer
                        {
                            string SQLString;
                            
                            SQLString = "INSERT INTO Coefficients (Run, RowNumber, Reach, Parameter, R2, NS, logNS, RMSE, AD, VR, KGE, CAT_B, CAT_C, Cat_Ca, Cat_Cb, DateStamp) VALUES (" +
                                            fields[0] + ", " +
                                            fields[1] + ",'" +
                                            reachName + "', '" +
                                            fields[2] + "', " +         //parameter name
                                            fields[3] + "," +           //R2
                                            fields[4] + ", " +          //Nash Sutcliffe
                                            fields[5] + ", " +          // log(NS)
                                            fields[6] + ", " +          //RMSE
                                            fields[8] + ", " +          //AD
                                            fields[9] + ", " +          //VR
                                            fields[13]  + ", " +        //KGE
                                            fields[14] + ", " +          //CAT_B
                                            fields[15] + ", " +          //CAT_C
                                            fields[16] + ", " +         //CAT_Ca
                                            fields[17] + ", " +         //CAT_Cb
                                            "Date())";                  //Date 
                                        using (StreamWriter SQLCheck = new StreamWriter("SQLCheck.txt"))
                                        {
                                            SQLCheck.WriteLine(SQLString);
                                        }
                                        SQLiteCommand tmp = new SQLiteCommand(SQLString, localConnection);
                                        try { tmp.ExecuteNonQuery(); }
                                        catch (Exception ex) { Console.WriteLine(ex.Message); }
                                        tmp.Dispose();
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
        }

        private void executeSQLCommand(string commandString)
        {
            SQLiteCommand tmp = new SQLiteCommand(commandString, localConnection);
            //localConnection.Open();
            try { tmp.ExecuteNonQuery(); }
            catch (SQLiteException ex) { Console.WriteLine(ex.Message); }
            
            //localConnection.Close();
        }

        private void writeParameter(int runID, int ParID, string textValue)
        {
            string insertCommandString;
            string numericValueString;

            try
            {
                double test = Convert.ToDouble(textValue);
                numericValueString = textValue;
            }
            catch { numericValueString = "NULL"; }

            insertCommandString = "INSERT INTO ParList (RunID, ParID, TextValue, NumericValue) VALUES (" 
                + runID.ToString() + ", " + ParID.ToString() + ",  '" + textValue + "', " + numericValueString + ");";
            SQLiteCommand tmp = new SQLiteCommand(insertCommandString, localConnection);
            //Console.WriteLine(insertCommandString);
            try { tmp.ExecuteNonQuery(); }
            catch (Exception ex) { Console.WriteLine(ex.Message); }
            tmp.Dispose();
        }

        private void writeParameterName(int ParID, string parName)
        {
            string insertCommandString;

            insertCommandString = "INSERT INTO ParNames (ParID, ParName) VALUES (" +  ParID.ToString() + ",  '" + parName + "');";
            SQLiteCommand tmp = new SQLiteCommand(insertCommandString, localConnection);
            try { tmp.ExecuteNonQuery(); }
            catch (SQLiteException ex) { Console.WriteLine(ex.Message); }
        }

        public void writeParameterSet(int runID, parameterSet pSet)
        {
            Console.WriteLine("Writing parameter set {0}", runID);
            //probably need to open a connection string
           try { localConnection.Open(); }
            catch (Exception ex) { Console.WriteLine(ex.Message); }

            int m = 0;
            foreach (ArrayList l in pSet)
            {
                //need to separate out comma-separated lists of land use types
                foreach (parameter p in l)
                {
                    string[] s = (p.stringValue()).Split(MCParameters.separatorChar);
                    foreach (string par in s)
                    {
                        writeParameter(runID, m++, par);
                    }
                }
            }
            localConnection.Close();
        }

        public void writeParameterNames(ParameterArrayList pal)
        {
            try { localConnection.Open(); }
            catch (SQLiteException ex) { Console.WriteLine(ex.Message); }

            //each row in the header is an integer followed by a field name
            string[] s = (pal.header.ToString()).Split('\n');
            int m = 0;
            foreach (string par in s)
            {
                int splitPos = par.IndexOf(MCParameters.separatorChar);
                string parName = par.Substring(splitPos + 1);
                writeParameterName(m++, parName);
            }
            localConnection.Close();
        }

        public void writeCoefficientWeights()
        {
            try { localConnection.Open(); }
            catch (SQLiteException ex) { Console.WriteLine(ex.Message); }

            using (StreamReader coefficientWeights = new StreamReader(MCParameters.coefficientsWeightFile))
            {
                string line;
                string SQLString;

                while ((line = coefficientWeights.ReadLine()) != null)
                {
                    string[] fields = line.Split(MCParameters.separatorChar);
                    SQLString = "INSERT INTO CoefficientWeights (CoefficientName,CoefficientWeight) VALUES ('" +
                    fields[0] + "', " + fields[1] + ");";

                    SQLiteCommand tmp = new SQLiteCommand(SQLString, localConnection);
                    try { tmp.ExecuteNonQuery(); }
                    catch (Exception ex) { Console.WriteLine(ex.Message); }
                    tmp.Dispose();
                }
            }
            localConnection.Close();
        }
    }
}
