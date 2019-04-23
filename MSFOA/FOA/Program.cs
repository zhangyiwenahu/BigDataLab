using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;

namespace FOA
{
    class Program
    {
        static void Main(string[] args)
        {
            double[] PG = new double[FOA.Iter];//保存每一次迭代时Pbest-Gbest的差值
            for (int i = 0; i < FOA.Iter; i++)
                PG[i] = 0;

            double [] bestp = new double[FOA.Dim];
           
            double fit = 0;
            double totalfit = 0;
            double SD = 0;
            double pro = 0;
            double[,] bf = new double[FOA.Iter,FOA.Dim + 1];
            for (int i = 0; i < FOA.Iter; i++)
            {
                for(int j = 0; j <= FOA.Dim;j++)
                {
                    bf[i,j]=0;
                }
            }

            FileStream frs = new FileStream(@"./result.txt", FileMode.Append);
            StreamWriter frw = new StreamWriter(frs);
            double minfit = 10000 ;
            for (int i = 0; i < 50; i++)//实验重复50遍，记录每次进化时的结果，然后后续进行平均操作，用以画图
            {
                fit = FOA.Getmax(ref bestp,ref bf,ref PG);
                if (minfit > fit)
                {
                    minfit = fit;
                }
           
                totalfit += fit;
                SD+=(fit-pro)*(fit-pro);
                Console.WriteLine("the best fit is {0}",fit);
         
            }
            SD = Math.Sqrt(SD/ 50);
            Console.WriteLine("Average fit is {0},SD is {1},minfit is {2}", totalfit / 50, SD, minfit);



           // frw.WriteLine("fitness of population :");结果保存至txt文档
            for (int i = 0; i < FOA.Iter; i++)
            {
                double a = bf[i, FOA.Dim]/50;
                frw.WriteLine(a.ToString());
            }
           
            frw.Close();
            frs.Close();
        }
    }
}
