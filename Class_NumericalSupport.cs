using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace MC
{
    class numericalSupport
    {
        //modeified from johndcook.com
        static double Phi(double x)
        {
            // constants    
            double a1 = 0.254829592;
            double a2 = -0.284496736;
            double a3 = 1.421413741;
            double a4 = -1.453152027;
            double a5 = 1.061405429;
            double p = 0.3275911;

            // Save the sign of x    
            int sign = 1;
            if (x < 0)
                sign = -1;
            x = Math.Abs(x) / Math.Sqrt(2.0);

            // A&S formula 7.1.26    
            double t = 1.0 / (1.0 + p * x);
            double y = 1.0 - (((((a5 * t + a4) * t) + a3) * t + a2) * t + a1) * t * Math.Exp(-x * x);
            return 0.5 * (1.0 + sign * y);
        }

        static void TestPhi()
        {
            // Select a few input values    
            double[] x = { -3, -1, 0.0, 0.5, 2.1 };
            // Output computed by Mathematica    
            // y = Phi[x]    
            double[] y = { 0.00134989803163, 0.158655253931, 0.5, 0.691462461274, 0.982135579437 };
            double maxError = 0.0;
            for (int i = 0; i < x.Length; ++i)
            {
                double error = Math.Abs(y[i] - Phi(x[i]));
                if (error > maxError)
                    maxError = error;
            }
        }

        // compute log(1+x) without losing precision for small values of x    
        static double LogOnePlusX(double x)
        {
            if (x <= -1.0)
            {
                string msg = String.Format("Invalid input argument: {0}", x);
                throw new ArgumentOutOfRangeException(msg);
            }
            if (Math.Abs(x) > 1e-4)
            {
                // x is large enough that the obvious evaluation is OK
                return Math.Log(1.0 + x);
            }

            // Use Taylor approx.
            // log(1 + x) = x - x^2/2 with error roughly x^3/3        
            // Since |x| < 10^-4, |x|^3 < 10^-12,         
            // relative error less than 10^-8        
            return (-0.5 * x + 1.0) * x;
        }

        static double RationalApproximation(double t)
        {
            // Abramowitz and Stegun formula 26.2.23.        
            // The absolute value of the error should be less than 4.5 e-4.        
            double[] c = { 2.515517, 0.802853, 0.010328 };
            double[] d = { 1.432788, 0.189269, 0.001308 };
            return t - ((c[2] * t + c[1]) * t + c[0]) /
                (((d[2] * t + d[1]) * t + d[0]) * t + 1.0);
        }

        public static double NormalCDFInverse(double p)
        {
            if (p <= 0.0 || p >= 1.0)
            {
                string msg = String.Format("Invalid input argument: {0}.", p);
                throw new ArgumentOutOfRangeException(msg);
            }

            // See article above for explanation of this section.        
            if (p < 0.5)
            {
                // F^-1(p) = - G^-1(p)            
                return -RationalApproximation(Math.Sqrt(-2.0 * Math.Log(p)));
            }
            else
            {
                // F^-1(p) = G^-1(1-p)
                return RationalApproximation(Math.Sqrt(-2.0 * Math.Log(1.0 - p)));
            }
        }

        public static void demo()
        {
            Console.WriteLine("\nShow NormalCDFInverse is accurate at");
            Console.WriteLine("0.05, 0.15, 0.25, ..., 0.95 ");
            Console.WriteLine("and at a few extreme values.\n");
            double[] p = { 0.0000001, 0.00001, 0.001, 0.05, 0.15, 0.25, 0.35, 0.45, 0.55, 0.65, 0.75, 0.85, 0.95, 0.999, 0.99999, 0.9999999 };
            // Exact values computed by Mathematica.        
            double[] exact = { -5.199337582187471, -4.264890793922602, -3.090232306167813, -1.6448536269514729, -1.0364333894937896, -0.6744897501960817, -0.38532046640756773, -0.12566134685507402, 0.12566134685507402, 0.38532046640756773, 0.6744897501960817, 1.0364333894937896, 1.6448536269514729, 3.090232306167813, 4.264890793922602, 5.199337582187471 };
            double maxerror = 0.0;
            Console.WriteLine("p, exact CDF inverse, computed CDF inverse, diff\n");
            for (int i = 0; i < exact.Length; ++i)
            {
                double computed = NormalCDFInverse(p[i]);
                double error = exact[i] - computed;
                Console.WriteLine("{0}, {1}, {2}, {3}", p[i], exact[i], computed, error);
                if (Math.Abs(error) > maxerror)
                    maxerror = Math.Abs(error);
            }
            Console.WriteLine("\nMaximum error: {0}\n", maxerror);
        }
    }
}
