using System;
using System.Collections.Generic;
using System.Collections;
using System.IO;
using System.Linq;
using System.Text;

namespace MC
{
    public class parameter
    {
        public virtual void write() { }
        public virtual void jump(int Offset, int Divisions) { }
        public virtual void update(string updateVal) { }
        public virtual void update(updateType u, double updateVal) { }
        public virtual void MCMCupdate(updateType u, double updateVal) { }
        public virtual string stringValue() { return "x"; }
        public virtual double CandidateMCMCJump(double f) { return 0.0; }
    }

    public enum updateType
    {
        add,
        subtract,
        replace
    };

    public class parameterDouble : parameter
    {
        protected double value, minimum, maximum;
        static protected Random r;

        public parameterDouble()
        {
            r=new Random();
        }

        public parameterDouble(double v, double min, double max)
        {
            value = v;
            minimum = min;
            maximum = max;
            r = new Random();
        }

        public override void jump(int Offset, int Divisions) 
        {
            // go to a random point between min + (offset/divisions)*(max-min) and min + ((offset +1))/divisions)*(max-min)
            // note that jumps should only occur when maximum != minimum
            double range = (this.maximum - this.minimum)/Convert.ToDouble(Divisions);
            if (range > 0)
            {
                double lowerBound = this.minimum + range* Convert.ToDouble(Offset);
                double val = lowerBound +  r.NextDouble() * range;
                this.value = val;
            }
        }
 
        public override void update(String updateVal)
        {
            this.value = Convert.ToDouble(updateVal);
        }

        public override void update(updateType u, double updateVal)
        {
            switch (u)
            {
                case updateType.add:
                    this.value += updateVal;
                    break;
                case updateType.subtract:
                    this.value -= updateVal;
                    break;
                case updateType.replace:
                    this.value = updateVal;
                    break;
            }
        }

        public override void MCMCupdate(updateType u, double updateVal)
        {
            switch (u)
            {
                case updateType.add:
                    this.value += updateVal;
                    break;
                case updateType.subtract:
                    this.value -= updateVal;
                    break;
                case updateType.replace:
                    this.value = updateVal;
                    break;
            }
            if ((this.value < this.minimum) || (this.value > this.maximum))
            {
                this.jump(0, 1);
                //Console.WriteLine("Range exceeded by jump");
            }
        }
        private double stdDev()
        {
            double above = (this.maximum - this.value);
            double below = (this.value - this.minimum);
            return above > below ? below : above;
        }

        public override double CandidateMCMCJump(double f)
        {
            double range = this.maximum - this.minimum;
            double stdDevToUse = stdDev() / 5.0;    // this should keep values from over-ranging
            if (f > 1.0) f = 1.0;                   // this should keep values from over-ranging

            double jump = numericalSupport.NormalCDFInverse(r.NextDouble());
            jump = jump * stdDevToUse * f;

            return jump;
        }

        public override string stringValue()
        {
            return this.value.ToString();
        }

        public parameterDouble Copy()
        {
            parameterDouble r = new parameterDouble(this.value, this.minimum, this.maximum);
            return r;
        }
    }

  
    public class parameterString : parameter
    {
        string value;

        protected parameterString()
        {
            value = "x";
        }

        public parameterString(string v)
        {
            value = v;
        }

        public override string stringValue()
        {
            return value;
        }

        public override void jump(int Offset, int Divisions) {  }

        public override void update(updateType u, double updateVal) { }

        public override double CandidateMCMCJump(double f)
        {
            return base.CandidateMCMCJump(f);
        }
   
        public parameterString Copy()
        {
            parameterString r = new parameterString(this.value);
            return r;
        }
    }
}
