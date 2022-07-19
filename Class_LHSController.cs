using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace MC
{   
    public class LHSController : ArrayList
    {
        public ArrayList LHSline;
        public int[] LHSArray;
        public int Divisions;

        public LHSController(int d, parameterSet pSet)
        {
            Divisions = d;

            foreach (ArrayList l in pSet)
            {
                LHSline=new ArrayList();

                foreach (parameter p in l)
                {
                    LHSArray = new int[Divisions];
                    for (var i = 0; i < Divisions; i++)
                        LHSArray[i] = i;
                    LHSline.Add(LHSArray);
                }
                this.Add(LHSline);
            }
        }

        public void shuffle()
        {
            foreach (ArrayList a in this)
            {
                foreach (int[] l in a)
                {
                    Permutations.FisherYatesShuffle(l);
                    //for (int k = 0; k < l.Length; k++)
                    //    Console.Write("{0} ", l[k]);
                    //Console.WriteLine();
                }
            }
        }
    }
}
