using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;

namespace AFOA
{
    class AFOA
    {
        private static   double Wmax = 0.95, Wmin = 0.15, Cmax = 2, Cmin = 0.5;
        private static int iter = 10000;
        private static int pop = 100;
        private static double rangemax = 20;
        private static double rangemin = 0;
        private static double rmax = 10;
        private static double  rmin = -10;
        private static int dim=1;//维度
        public static int Dim
        {
            set
            {
                dim = value;
            }
            get
            {
                return dim;
            }
        }
        public static int Iter
        {
            set
            {
                iter = value;
            }
            get
            {
                return iter;
            }
        }
        public static double GetMax(ref double[] bestx,ref double[,]pf,ref double[] PG)
        {
          
            double fit = 1000000;
            List<double>[] wlist = new List<double>[pop];
            for (int i = 0; i < pop; i++)
            {
                wlist[i] = new List<double>();
            }
            for (int j = 0; j < pop; j++)
            {
                for (int i = 0; i < dim; i++)
                    wlist[j].Add(new double());
            }
            double[] vx = new double[dim];
            double[] px = new double[dim];
            Random radd = new Random();
            //初始化
            for (int i = 0; i < dim; i++)
            {
                vx[i] = rangemin+(rangemax - rangemin) * radd.NextDouble();
                px[i] =  (rangemin + (rangemax - rangemin) * radd.NextDouble());             
            }
            double w, c;
            for (int t = 0; t < iter; t++)
            {
                Random rad=new Random();
                for (int i = 0; i < dim; i++)
                    pf[t, i] += px[i];
                pf[t, dim] += fit;
                
                double pfit=100000;
                double[] pxt = new double[dim];//pbest              
                for (int i = 0; i < pop; i++)
                {
                    double fiti = 0;
                    double t1 = 0, t2 = 0;
                    //一般可迭代多维函数

                    for (int j = 0; j < dim; j++)
                    {
                        wlist[i][j] = px[j] + vx[j] * (2 * rad.NextDouble() - 1);
                        while (wlist[i][j] > rangemax)
                        {
                            if (rangemax > 0)
                                wlist[i][j] -= rangemax;
                            else
                                if (rangemax == 0)
                                    wlist[i][j] = 0;
                                else
                                    wlist[i][j] += rangemax;
                        }
                        while (wlist[i][j] < rangemin)
                        {
                            if (rangemin < 0)
                                wlist[i][j] -= rangemin;
                            else
                                if (rangemin == 0)
                                    wlist[i][j] = 0;
                                else
                                    wlist[i][j] += rangemin;
                        }

                        fiti = wlist[i][j] * Math.Sin(wlist[i][j]);

                        //fiti += wlist[i][j] * wlist[i][j];//f1,f2
                        //fiti = Math.Sin(wlist[i][j]);//f5

                        //t2 += Math.Abs(wlist[i][j]);
                        //t1 *= Math.Abs(wlist[i][j]);//F3

                        //fiti = Math.Abs(wlist[i][j]);//f4
                        //fiti += (-1 * wlist[i][j]) * Math.Sin(Math.Sqrt(Math.Abs(wlist[i][j]))); //f10
                        //fiti += (wlist[i][j] * wlist[i][j]) - 10 * Math.Cos(2 * Math.PI * wlist[i][j]) + 10;//f11

                        //t1 += wlist[i][j] * wlist[i][j];
                        //t2 += Math.Cos(2 * Math.PI * wlist[i][j]);//F12

                        //t1 += wlist[i][j] * wlist[i][j];
                        //t2 *= Math.Cos(wlist[i][j] / Math.Sqrt(j + 1));//F13

                    }
                    //fiti = t2 + t1;//F3

                    //fiti = -20 * Math.Exp(-0.2 * Math.Sqrt(t1 / dim)) - Math.Exp(t2 / dim) + 20 + Math.E;//F12

                    //fiti = 1.0 / 4000 * t1 + 1 - t2;//f13

                    if (pfit > fiti)//min
                    {
                        pfit = fiti;
                        for (int j = 0; j < dim; j++)
                            pxt[j] = wlist[i][j];
                    }

                    ////二维函数复合计算，
                    //wlist[i][0] = px[0] + vx[0] * (2 * rad.NextDouble() - 1);

                    //while (wlist[i][0] > rangemax)
                    //{
                    //    if (rangemax > 0)
                    //        wlist[i][0] -= rangemax;
                    //    else
                    //        if (rangemax == 0)
                    //            wlist[i][0] = 0;
                    //        else
                    //            wlist[i][0] += rangemax;
                    //}
                    //while (wlist[i][0] < rangemin)
                    //{
                    //    if (rangemin < 0)
                    //        wlist[i][0] -= rangemin;
                    //    else
                    //        if (rangemin == 0)
                    //            wlist[i][0] = 0;
                    //        else
                    //            wlist[i][0] += rangemin;
                    //}
                    //wlist[i][1] = px[1] + vx[1] * (2 * rad.NextDouble() - 1);
                    //while (wlist[i][1] > rmax)
                    //{
                    //    if (rmax > 0)
                    //        wlist[i][1] -= rmax;
                    //    else
                    //        if (rmax == 0)
                    //            wlist[i][1] = 0;
                    //        else
                    //            wlist[i][1] += rmax;
                    //}
                    //while (wlist[i][1] < rmin)
                    //{
                    //    if (rmin < 0)
                    //        wlist[i][1] -= rmin;
                    //    else
                    //        if (rmin == 0)
                    //            wlist[i][1] = 0;
                    //        else
                    //            wlist[i][1] += rmin;
                    //}


                    ////fiti = (1 + (wlist[i][0] + wlist[i][1] + 1) * (wlist[i][0] + wlist[i][1] + 1) * (19 - 14 * wlist[i][0] + 3 * wlist[i][0] * wlist[i][0] - 14 * wlist[i][1] + 6 * wlist[i][0] * wlist[i][1] + 3 * wlist[i][1] * wlist[i][1])) * (30 + (2 * wlist[i][0] - 3 * wlist[i][1]) * (2 * wlist[i][0] - 3 * wlist[i][1]) * (18 - 32 * wlist[i][0] + 12 * wlist[i][0] * wlist[i][0] + 48 * wlist[i][1] - 36 * wlist[i][0] * wlist[i][1] + 27 * wlist[i][1] * wlist[i][1]));//F6
                    ////fiti = wlist[i][0] * wlist[i][0] + wlist[i][1] * wlist[i][1] - Math.Cos(18 * wlist[i][0]) - Math.Cos(18 * wlist[i][1]);//F8
                  
                    //for (int j = 1; j <= 5; j++)
                    //{
                    //    t1 += j * Math.Cos((j + 1) * wlist[i][0] + j);
                    //    t2 += j * Math.Cos((j + 1) * wlist[i][1] + j);
                    //}
                    //fiti = t1 * t2;//F9
                    ////fiti = (wlist[i][1] - 5.1 / (4 * Math.PI * Math.PI) * wlist[i][0] * wlist[i][0] + 5 / Math.PI * wlist[i][0] - 6) * (wlist[i][1] - 5.1 / (4 * Math.PI * Math.PI) * wlist[i][0] * wlist[i][0] + 5 / Math.PI * wlist[i][0] - 6) + 10 * (1 - 1 / (8 * Math.PI)) * Math.Cos(wlist[i][0]) + 10;//F7

                    ////Console.WriteLine("&&&&&&&&&&&   fiti={0} &&&&&&& ",fiti);
                    //if (pfit > fiti)
                    //{
                    //    pfit = fiti;
                    //    pxt[0] = wlist[i][0];
                    //    pxt[1] = wlist[i][1];
                    //}
                }

                
                if (fit > pfit)
                {
                    fit = pfit;
                    for (int i = 0; i < dim; i++)
                    {
                        bestx[i] = pxt[i];
                    }
                }
                w = Wmin + (Wmax - Wmin) * (iter - t) / iter;
                c = (Cmax - Cmin) * t / iter;
                for (int i = 0; i < dim; i++)
                {
                    double temp = (bestx[i] + pxt[i]);
                    vx[i] = w * vx[i] + 2*c * rad.NextDouble() * (temp - px[i]) ;//pxt表示第t次迭代时种群最优，bestx表示种群历史最优解，px表示当次种群位置
                    //if (vx[i] > rangemax * 0.2)
                    //{
                    //    vx[i] = rangemax * 0.2;
                    //}
                    //if (vx[i] < rangemin * 0.2)
                    //{
                    //    vx[i] = rangemin * 0.2;
                    //}
                }
                for (int i = 0; i < dim; i++)
                {
                    px[i] = bestx[i];
                }
              
                PG[t] += pfit - fit;

           
            }

            return fit;
        }
    }
}
