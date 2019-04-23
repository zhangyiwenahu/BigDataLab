using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace FOA
{
    class Mutation
    {
        public static double[] Mutat(double[] x,int dim,double [] fit,int M,double[] SD,double rangemax, double rangemin, int t,Random radd)//其中，x表示种群位置，dim表示维度，fit表示种群适应度集合，M表示尺度个数，SD表示每个子群的标准差，rangemax,rangemin表示定义域范围
        {
            double[] X=new double [dim];//表示新产生的种群位置
           
            double[,] Fit = new double[M, fit.Length / M];
            double[] FitX = new double[M];
          
            double SDmax=0, SDmin=1000, SDsum=0;

            for (int i = 0; i < fit.Length; i++)//fit排序
            {
                for (int j = i + 1; j < fit.Length; j++)
                {
                    if (fit[i] > fit[j])
                    {
                        double temp = fit[i];
                        fit[i] = fit[j];
                        fit[j] = temp;
                    }
                }
            }
            //fit划分M个子群
            for (int i = 0; i < M; i++)
            {
                for (int j = 0; j < fit.Length/M; j++)
                {
                    Fit[i, j] = fit[i * fit.Length/M + j];
                }
            }

            //计算每个子群的适应度
            for (int i = 0; i < M; i++)
            {
                for(int j=0;j<fit.Length/M;j++)
                {
                    FitX[i] += Fit[i,j];
                }
                FitX[i] /= fit.Length / M ;
            }
           
            //计算每个子群的标准差
            for (int i = 0; i < M; i++)
            {
                SDsum += FitX[i];
                if (FitX[i] > SDmax)
                    SDmax = FitX[i];
                if (FitX[i] < SDmin)
                    SDmin = FitX[i];
            }
            for (int i = 0; i < M; i++)
            {
                SD[i] = SD[i] * Math.Exp((t * Math.Abs(M * FitX[i] - SDsum)) / ((FOA.Iter - t) * (SDmax - SDmin)));//(M * FitX[i] - SDsum) / (SDmax - SDmin)
                //判断每个标准差是否过大

                if (SD[i] > (rangemax - rangemin) / 4)
                {
                    SD[i] = (rangemax-rangemin)/4;
                }
            }

            //改变初始种群位置
            
            double [] tempx=new double[dim];
    

            double ff=1000;
            for (int j = 0; j < M; j++)//获得每个子群的最小适应度值
            {
                double[] tempjx = new double[dim];
    
                double[] d = new double[dim];
                double[] S = new double[dim];
                for (int i = 0; i < dim; i++)
                {
                    tempjx[i]= x[i] + radd.NextDouble() * SD[j];
                }
                double fj=fitness.Getfit(dim, S);
                if (ff >fj )
                {
                    ff = fj;
                    for (int k = 0; k < dim; k++)
                    {
                        tempx[k] = tempjx[k];
                    }
                }
            }
            double[] tempwx=new double[dim];//表示定义域范围内随机产生的位置
          
            double[] dw = new double[dim];
            double[] Sw = new double[dim];
            for (int i = 0; i < dim; i++)
            {
                tempwx[i] = x[i] + radd.NextDouble() * (rangemax - rangemin);
              
            }
            if (ff < fitness.Getfit(dim, Sw))
            {
                for (int i = 0; i < dim; i++)
                {
                    X[i]= tempx[i];
                }
            }
            else
            {
                for (int i = 0; i < dim; i++)
                {
                    X[i] = tempwx[i];
                }
            }

            return X;
        }
    }
}
