using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace MC
{
    public class performanceStatistic
    {
        public string name;
        public bool[] flags;
        public double weight;

        public virtual double valueToUse(string inputString)
        {
            double value;
            try
            {
                value = Convert.ToDouble(inputString);
            }
            catch (Exception ex)
            {
                value = 0;
                Console.WriteLine(ex.Message);
            }
            return value;
        }

        protected performanceStatistic()
        {
            //generic initializer of validity
            //note that values[0] does nothing, it is jut a useful placeholder to allow for the model id
            //to be used as an indexer for validity
            flags = new bool[]{ false, false, false, false, false, false, false, false };
        }

        protected double returnMinusOne(string inputString)
        {
            //note that things may behave in a very odd way if NS crosses 0.
            //Maximizing NS-1 will prevent this from happenning
            double val;
            try
            {
                val = Convert.ToDouble(inputString);
                val = val - 1;
            }
            catch
            {
                val = 0.0;
            }
            return val;
        }

        protected double returnNegative(string inputString)
        {
            double val;
            try
            {
                val = Convert.ToDouble(inputString);
                val = -1.0 * val;
            }
            catch
            {
                val = 0.0;
            }
            return val;
        }
    }

    class nashSutcliffe : performanceStatistic
    {
        override public double valueToUse(string inputString)
        {
           return returnMinusOne(inputString);
        }

        nashSutcliffe()
        {
            name = "Nash Sutcliffe";
            // Nash Sutcliffe is valid for every model
            for (var i=0; i<flags.Length; i++)
            {
                flags[i] = true;
            }
        }
    }

    class logNashSutcliffe : performanceStatistic
    {
        override public double valueToUse(string inputString)
        {
            return returnMinusOne(inputString);
        }

        logNashSutcliffe()
        {
            name = "log(Nash Sutcliffe)";

            flags[1] = true;    //valid for PERSiST
        }
    }

    class R2: performanceStatistic
    {
        public override double valueToUse(string inputString)
        {
            return returnNegative(inputString);
        }

        R2()
        {
            name = "Pearson Correlation";
            //R2 is valid for every model
            for (var i = 0; i < flags.Length; i++)
            {
                flags[i] = true;
            }
        }
    }

    class RMSE: performanceStatistic
    {
        public override double valueToUse(string inputString)
        {
            return returnNegative(inputString);
        }

        RMSE()
        {
            name = "Root Mean Square Error";
            //RMSE is valid for every model
            for (var i = 0; i < flags.Length; i++)
            {
                flags[i] = true;
            }
        }
    }

    class AD: performanceStatistic
    {
        public override double valueToUse(string inputString)
        {
            double val;
            try
            {
                val = Convert.ToDouble(inputString);
                val = -1.0 * Math.Abs(val);
            }
            catch
            {
                val = 0.0;
            }
            return val;
        }

        AD()
        {
            name = "Absolute Difference";

            flags[1] = true;    //valid for PERSiST
        }
    }
}
