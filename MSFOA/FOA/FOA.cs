using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;

namespace FOA
{
    class FOA
    {
        private static int M = 5;
        private static double[] SD = new double[M];
        private static int iter = 300;
        private static int pop = 50;


        private static int dim = 50;

        private static double n = 0.005, omiga = 1, alfa = 0.95;
        private static double rangemax =100, rangemin = -100;//定义域
        
        public static int Dim
        {
            get
            {
                return dim;
            }
            set
            {
                dim = value;
            }
        }
        public static int Iter
        {
            get
            {
                return iter;
            }
            set
            {
                iter = value;
            }
        }
        public static double Getmax(ref double[] bestx,ref double[,] pf,ref double[] PG)
        {

            for (int i = 0; i < M; i++)//初始化每个子种群的均方差
                SD[i] = rangemax - rangemin;

            double fit = 100000;
            double w;
            double[] fiti=new double[pop];
            
            List<double>[] wlist = new List<double>[pop];
            for (int i = 0; i < FOA.pop; i++)
                wlist[i] = new List<double>();
            for (int i = 0; i < pop; i++)
                for (int j = 0; j < dim; j++)
                    wlist[i].Add(new double());

            double[] px = new double[dim];       
            double[,] s = new double[pop, dim];
            
      
            //px = GetInit();初始化种群位置
            int flag = 0;
            Random radd = new Random();
            for (int i = 0; i < dim; i++)
            {
                px[i] = n*(rangemin + (rangemax - rangemin) * radd.NextDouble());
            }
            for (int t = 0; t < iter; t++)
            {
                double pfit=100000;

                double[] pxt = new double[dim];  
                //结果保存
                pf[t, dim] += fit;//初始化每个测试函数的值

                w = omiga * Math.Pow(alfa, t);
                for (int i = 0; i < pop; i++)
                {
                    
                    double[] tempx = new double[dim];
                    //一般可迭代多维函数
                    for (int j = 0; j < dim; j++)
                    {
                        //获取每个个体
                        wlist[i][j]= px[j] + w*(rangemin + (rangemax - rangemin) * radd.NextDouble());
                        s[i, j] = wlist[i][j];
                        //越界判断
                        while (s[i,j] > rangemax)
                        {
                            if (rangemax > 0)
                                s[i,j] -= rangemax;
                            else
                                if (rangemax == 0)
                                    s[i,j] = 0;
                                else
                                    s[i,j] += rangemax;
                        }
                        while (s[i,j] < rangemin)
                        {
                            if (rangemin < 0)
                                s[i,j] -= rangemin;
                            else
                                if (rangemin == 0)
                                    s[i,j] = 0;
                                else
                                    s[i,j] += rangemin;
                        }

                        tempx[j] = s[i, j];
                    }
                    fiti[i] = fitness.Getfit(dim, tempx);
                    //种群最优保存
                    if (pfit > fiti[i])//min
                    {
                        pfit = fiti[i];//Pbest--fit
                        for (int j = 0; j < dim; j++)
                        {
                            pxt[j] = wlist[i][j];//Pbest--position
                        }
                    }
                }
                //全体最优保存
                double fittemp=fit;
                if (fit > pfit)
                {
                    fit = pfit;//Gbest--fit
                    for (int i = 0; i < dim; i++)
                    {
                        bestx[i]= px[i];//Gbest--position
                    }
                }
                if ( fit- fittemp==0)//
                {
                    if (flag >= dim/2)
                    {
                        //满足条件进行变异操作
                        px = Mutation.Mutat(px, dim, fiti, M, SD, rangemax, rangemin, t,radd);
         
                        flag = 0;
                    }
                    else
                    {
                        for (int i = 0; i < dim; i++)
                        {
                            px[i] = bestx[i];
                        }
                        flag++;
                        
                    }
                }
                else
                {
                    for (int i = 0; i < dim; i++)
                    {
                        px[i]= bestx[i];                     
                    }
                    flag = 0;
                }
            }

            return fit;
        }
        public static double[] GetInit()//初始化种群位置
        {
            double[] p = new double[dim];
            Random rad = new Random();
            for (int i = 0; i < dim; i++)
            {
                p[i] = n*(rangemax - rangemin) * rad.NextDouble() + rangemin;
            }
            return p;
        }
    }
}
