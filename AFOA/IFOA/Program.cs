using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;

namespace AFOA
{
    class Program
    {
        static void Main(string[] args)
        {
            double[] PG = new double[AFOA.Iter];//保存每一次迭代时Pbest-Gbest的差值
            for (int i = 0; i < AFOA.Iter; i++)
                PG[i] = 0;

            //Position bestp = new Position();
            double[] bestp = new double[AFOA.Dim];
            double fit = 0;
            double totalfit = 0;
            double SD = 0;
            double pro = -17.3076;
            double[,] pf = new double[AFOA.Iter, AFOA.Dim+1];//最后一个存放的是对应的适应度值，前dim个表示对应迭代次数时种群的位置，dim表示维数
            for (int i = 0; i < AFOA.Iter; i++)
                for (int j = 0; j <= AFOA.Dim; j++)
                    pf[i, j] = 0;

            FileStream frs = new FileStream(@"./result.txt", FileMode.Append);
            StreamWriter frw = new StreamWriter(frs);
            for (int i = 0; i < 50; i++)
            {
                fit = AFOA.GetMax(ref bestp,ref pf, ref PG);
                totalfit += fit;

                SD+=(fit-pro)*(fit-pro);

                Console.WriteLine("the best fit is {0}", fit);
                //Console.WriteLine("the best position is ({0},{1})", bestp.X, bestp.Y);
                for (int j = 0; j < AFOA.Dim; j++)
                {
                    Console.Write("{0},", bestp[j]);
                }
                Console.WriteLine("***********************************");
            }
            SD = Math.Sqrt(SD / 50);
            Console.WriteLine("Average fit is {0},std is {1}", totalfit / 50,SD);
   
            frw.WriteLine("fitness of population :");
            for (int i = 0; i < AFOA.Iter/50; i++)
            {
                double a = pf[i*50, AFOA.Dim] / 50;
                frw.WriteLine(a.ToString());
            }
            frw.Close();
            frs.Close();
        }
    }
}
