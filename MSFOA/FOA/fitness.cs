using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace FOA
{
    class fitness
    {
        //用以测试不同的函数。
        public static double Getfit(int dim, double[] x)
        {
            double fit = 0;
            double t1=0;
            double t2 = 0;
            Random rad = new Random();

            fit += (x[0] - 1) * (x[0] - 1);//F2

            //t1+=(x[0]-1)*(x[0]-1);//F23
            for (int i = 0; i < dim; i++)
            {
                //fit += (i + 1) * x[i] * x[i];//F1.F13

                //fit += (2 * x[i] * x[i] - x[i - 1]) * (2 * x[i] * x[i] - x[i - 1]);//F2
               
                //fit += x[i] * x[i];//F3,F14

                //fit += Math.Pow(Math.Pow(10, 6), i /(dim - 1)) * (x[i] * x[i]);  //F4
               
                //fit += (i + 1) * Math.Pow(x[i], 4);//F5
                
                //fit += 100 * Math.Pow((x[i + 1] - x[i] * x[i]), 2) + (x[i] - 1) * (x[i] - 1);//F6
                
                //double tfit = 0;//F7,F15
                //for (int j = 0; j < i; j++)//F7,F15
                //{
                //    tfit += x[j];//F7,F15
                //}
                //fit += tfit*tfit;//F7,F15

                //if (fit < Math.Abs(x[i]))//F8
                //{
                //    fit = Math.Abs(x[i]);//F8
                //}//F8

                //t1 += Math.Abs(x[i]);//F9
                //t2 *= Math.Abs(x[i]);//F9

                //fit += x[i] * x[i];//F10

                //fit += (int)(x[i] + 0.5) * (int)(x[i] + 0.5); //F11

                //fit += Math.Pow(Math.Abs(x[i]), i + 2); //F12

                //t1 += x[i] * x[i];//F16
                //t2 += Math.Cos(2 * Math.PI * x[i]);//F16

                //fit+=Math.Abs(x[i]*Math.Sin(x[i])+0.1*x[i]); //F17

                //fit += Math.Pow((x[i] * x[i] + x[i + 1] * x[i + 1]), 0.25) * (Math.Pow(Math.Sin(50 * Math.Pow((x[i] * x[i] + x[i + 1] * x[i + 1]), 0.1)), 2) + 1);//F18
                
                //fit+=0.5+(Math.Pow(Math.Sqrt(x[i]*x[i]+x[i+1]*x[i+1]),2)-0.5)/Math.Pow((1+0.001*(x[i]*x[i]+x[i+1]*x[i+1])),2);//F19
                
                //t1+=Math.Pow((0.25*(x[i]+1)),2)*(1+10*Math.Pow(Math.Sin(Math.PI*(1+0.25*(x[i+1]+1))),2));//F20
                //if (x[i] > 10)//F20
                //{
                //    t2 += 100 * Math.Pow(x[i] - 10, 4);//F20
                //}
                //else if (x[i] >= -10 && x[i] <= 10)//F20
                //{
                //    t2 += 0;//F20
                //}
                //else
                //{
                //    t2 += 100 * Math.Pow((-1 * x[i] - 10), 4);//F20
                //}

                //t1 += x[i] * x[i];//F21
                //t2 *= Math.Cos(x[i] / Math.Sqrt(i + 1));//F21

                //fit += -1 * Math.Exp(-1 * (x[i] * x[i] + x[i + 1] * x[i + 1] + 0.5 * x[i] * x[i + 1]) / 8) * Math.Cos(4 * Math.Sqrt(x[i] * x[i] + x[i + 1] * x[i + 1] + 0.5 * x[i] * x[i + 1]));//F22

                //t1+=(x[i]-1)*(x[i]-1);//F23
                //t2 += x[i] * x[i - 1];//F23

                //fit += Math.Pow((0.5+(Math.Pow(Math.Sin(Math.Sqrt(100*x[i]*x[i]+x[i+1]*x[i+1])),2)-0.5)/(1+0.001* Math.Pow((x[i]*x[i]-2*x[i]*x[i+1]+x[i+1]*x[i+1]),2))),2);//F24
               
                //fit+=x[i]*x[i]-10*Math.Cos(2*Math.PI*x[i])+10 ;//F25

                //if (Math.Abs(x[i]) < 0.5)//F26
                //{
                //    fit += x[i] * x[i] - 10 * Math.Cos(2 * Math.PI * x[i]) + 10;//F26
                //}
                //else//F26
                //{
                //    fit += (Math.Round(2 * x[i]) / 2) * (Math.Round(2 * x[i]) / 2) - 10 * Math.Cos(2 * Math.PI * Math.Round(2 * x[i]) / 2) + 10;//F26
                //}

                //t1 += x[i] * x[i];//F27

                //t1 = 0;//F28
                //for (int j = 0; j <= 30; j++)//F28
                //{
                //    t1 += Math.Pow(0.5, j) * Math.Cos(2 * Math.PI * Math.Pow(3, j) * (x[i] + 0.5));//F28
                //}
                //fit += t1;//F28

                //for (int j = 0; j < dim; j++)
                //{
                //    fit += Math.Pow((100 * Math.Pow((x[i] - x[j] * x[j]), 2) + Math.Pow((1 - x[j] * x[j]), 2)), 2) / 4000 - Math.Cos(100 * Math.Pow((x[i] - x[j] * x[j]), 2) + Math.Pow((1 - x[j] * x[j]), 2)) + 1;
                //}


            }
            //fit = -1 * Math.Exp(-0.5 * fit); //F3
            //fit = t1 + t2;//F9
            //fit += -450;//F14,F15
            //fit = -20 * Math.Exp(-0.2 * Math.Sqrt(t1 / dim)) - Math.Exp(t2 / dim) + 20 + Math.E;//F16
            //fit += Math.Pow((x[dim-1] * x[dim-1] + x[0] * x[0]), 0.25) * (Math.Pow(Math.Sin(50 * Math.Pow((x[dim-1] * x[dim-1] + x[0] * x[0]), 0.1)), 2) + 1);//F18
            //fit += 0.5 + (Math.Pow(Math.Sqrt(x[dim-1] * x[dim-1] + x[0] * x[0]), 2) - 0.5) / Math.Pow((1 + 0.001 * (x[dim-1] * x[dim-1] + x[0] * x[0])), 2);//F19

            //if (x[dim - 1] > 10)//F20
            //{
            //    t2 += 100 * Math.Pow(x[dim - 1] - 10, 4);//F20
            //}
            //else if (x[dim - 1] >= -10 && x[dim - 1] <= 10)//F20
            //{
            //    t2 += 0;//F20
            //}
            //else//F20
            //{
            //    t2 += 100 * Math.Pow((-1 * x[dim - 1] - 10), 4);//F20
            //}

            //fit = Math.PI / dim * ((10 * Math.Pow(Math.Sin(Math.PI * (1 + 0.25 * (x[0] + 1))), 2)) + t1 + (x[dim - 1] + 1) * (x[dim - 1] + 1) / 16) + t2;//F20
            //fit = 1.0 / 4000 * t1 - t2 + 1;//F21
            //fit = t1 - t2;//F23

            //fit = 1 - Math.Cos(2 * Math.PI * Math.Sqrt(t1)) + 0.1 * Math.Sqrt(t1);//F27

            //for (int j = 0; j <= 30; j++)//F28
            //{
            //    t2 += Math.Pow(0.5, j) * Math.Cos(2 * Math.PI * Math.Pow(3, j) * 0.5);//F28
            //}
            //fit -= dim * t2;//F28


            return fit;
        }
    }
}
