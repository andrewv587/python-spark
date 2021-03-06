#!/usr/bin/python
# -*- coding:utf-8 -*-  
#Filename:na-sa.py
#Function:
#Author:Huang Weihang
#Email:huangweihang14@mails.ucas.ac.cn
#Data:2016-12-28

from numpy import *
from matplotlib.pyplot import *
from pyspark import SparkConf, SparkContext

def findData(input_data,bdcdatasum,ns,nr,n_par,lowb,upperb):
    datasum=bdcdatasum.value
    oridata=[]
    for j in xrange(size(input_data,0)):
       vdata=input_data.copy()
       tmpdata=vdata.copy()
       for kk in xrange(int(ns/nr)):
           for i in xrange(n_par):  #进行n_par个坐标循环
               # print('  i='+str(i))
               range1=[lowb[i],upperb[i]]
               tmp2data=tmpdata.copy()
               tmp2data[i]=vdata[i]
               vji=vdata[i]
               dj2=sum((vdata-tmp2data)**2)
               for k in xrange(size(datasum,0)):
                   if sum((datasum[k,:]-inputdata)**2)<1e10:
                       continue
                   tmp2data[i]=datasum[k,i]
                   vki=datasum[k,i]
                   dk2=sum((datasum[k,:]-tmp2data)**2)
                   if abs(vki-vji)<1e-10:
                       continue
                   xji=1/2.*(vki+vji+(dk2-dj2)/(vki-vji))#计算xj的第i个分量
                   if xji>range1[0] and xji<=tmpdata[i]:
                       range1[0]=xji
                   elif xji<range1[1] and xji>=tmpdata[i]:
                       range1[1]=xji
               newx_loc=range1[0]+(range1[1]-range1[0])*random.rand() #均分分布生成新的坐标
               tmpdata[i]=newx_loc
           oridata=list(oridata)+[list(tmpdata)]#该次迭代生成的ns个样本
    return oridata

APP_NAME="na-spark"
conf=SparkConf().setAppName(APP_NAME)
sc=SparkContext(conf=conf)
par_num=3

n_itr=20     #迭代次数
ns=50         #ns参数
nr=10        #nr参数 可设为自适应参数
n_par=2       #参数个数
lowb=asarray([-10,-10])  #下边界
upperb=asarray([10,10])  #上边界
x_idx=0       #显示i分量为x轴
y_idx=1       #显示j分量为y轴
debug=0       #是否开启调试模式,0关闭，1开启（n_par~=2时自动关闭）
method='NA'    #'MC' 蒙特卡洛算法,'NA' 邻近算法
# ##---------------------------------------------------------------
#目标函数设置
##---------------------------------------------------------------
f=lambda x:(x[0]-2.5)**2+(x[1]+2)**2  #目标函数
#测试函数 参照Page153 MATLAB优化算法安全分析与应用
# f=lambda x:0.5-(sin(sqrt(x(1)**2+x(2)**2))**2-0.5)/ ...
#      (1+0.001*(x(1)**2+x(2)**2))**2  #Schaffer函数
# f=lambda x:-100*(x[1]-x[0]**2)**2-(x[0]-1)**2 #Rosenbrock函数
# f=lambda x:1/4000*sum(x.**2)-prod(cos(x./(sqrt(1:length(x)))))+1 #Griewank函数
# f=lambda x:sum(asarray(x)**2-10*cos(2*pi*asarray(x))+10) #Rastrigin函数
# f=lambda x:-20*exp(-0.2*sqrt(1/length(x)*sum(x.**2)))-exp(1/length(x)* ...
#     sum(cos(2*pi.*x)))+exp(1)+20  #Ackley函数
##---------------------------------------------------------------

#确保lowb、upperb与n_par一致
if len(lowb)!=n_par:
    print('lowb is not the same length as n_par')
    exit()
if len(upperb)!=n_par:
    print('upperb is not the same length as n_par')
    exit()

# 确保上边界大于下边界
if sum(lowb>=upperb)>0:
    print('lowb is larger than upperb')
    exit()

#参数个数大于2，关闭debug模式
if n_par!=2:
    print('Can''t open debug mode!')
    debug=0

#生成图形，并指定坐标轴范围；
figure(figsize=(4,4))
axis([lowb[x_idx],upperb[x_idx],lowb[y_idx],upperb[y_idx]])
grid(True)
box(True)
hold(True)
xlabel(str(x_idx)+' component')
ylabel(str(y_idx)+' component')

#参数为2且不为debug的情况下作出目标函数f(x)的等值图
if n_par==2 and debug!=1:
    x=linspace(lowb[0],upperb[0],100)
    y=linspace(lowb[1],upperb[1],100)
    X,Y=meshgrid(x,y)
    z=zeros_like(X)
    for i in xrange(0,size(X,0)):
        for j in xrange(0,size(X,1)):
            z[i,j]=f([X[i,j],Y[i,j]])
    contourf(X,Y,z)
    colorbar()

#Monte Carlo方法
if method=='MC':
    data=zeros([ns*nr+nr,n_par])
    misfit=zeros(ns*nr+nr)
    for i in xrange(0,(ns*nr+nr)):
        data[i,:]=lowb+(upperb-lowb)*random.rand(1,n_par)
        misfit[i]=f(data[i,:])
    title('Monte Carlo with point nums='+str(ns*nr+nr))
    scatter(data[:,x_idx],data[:,y_idx])
    chdata=amin(misfit)
    chindex=argmin(misfit)
    print('data and misfist is:')
    print(data[chindex,:],chdata)
    show()
    exit()
##---------------------------------------------------------------
# NA算法
##---------------------------------------------------------------
data=zeros([ns,n_par])
misfit=zeros(ns)
#随机生成ns个样本，并绘图
for i in xrange(ns):
    data[i,:]=lowb+(upperb-lowb)*random.rand(1,n_par)
    misfit[i]=f(data[i,:])
title('neighbourhood algoritthm with ns='+str(ns)+' nr='+str(nr))
# scatter(data[:,x_idx],data[:,y_idx])
# show()
# exit()


#选择nr个样本点，生成ns个样本
datasum=data.copy()
for itr in xrange(1,n_itr+1):
   print('iteration number is '+str(itr))
   if  itr!=1:
       data=oridata.copy()
   for i in xrange(size(data,0)):
       misfit[i]=f(data[i,:])
   #在前一次迭代生成的ns个样本中选择misfit最小的nr个
   bdcdatasum=sc.broadcast(datasum)
   chdata=sort(misfit)
   chindex=argsort(misfit)
   oridata=[]
   print('data: ',str(data[chindex[0],:]))
   print('misfit is ',str(chdata[0]))
   par_data=data[chindex[:nr,:]].copy() #test
   par_value=sc.parallelise(par_data,par_num).flatmap(lambda input_data:
        findData(input_data,bdcdatasum,ns,nr,n_par,lowb,upperb))
   oridata=asarry(par_value.collect()) #must test
   datasum=asarray(list(datasum)+list(oridata)) #生成的总样本数

scatter(datasum[:,x_idx],datasum[:,y_idx])
show()
