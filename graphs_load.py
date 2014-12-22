from pylab import *
x_5 = [1,2,4,8]
x_1 = [2,4,8]
x_2 = [4,8]
##load 
#500
y_5 = [179.515, 82.282,  37.23633333, 28.18466667]
#yerr_5 = [1.271573435, 3.092051746, 1.857345507, 3.880110866]
yerr_5 = [2.536, 6.156,   3.223,   7.755]

#1
y_1 = [182.8423333, 86.37433333, 54.93863333]
#yerr_1 =[2.944109769, 3.800729711, 4.95324206 ]
yerr_1 =[5.723,  7.583,   9.4771]

#2
y_2 = [188.3933333, 125.9313333]
#yerr_2 = [7.513769516 ,  8.003610768]
yerr_2 = [14.148,    15.454]

xtheory = [1,2,4,8]
ytheory = [256, 128, 64, 32]

figure()
scatter(x_5,y_5)
scatter(x_1,y_1)
scatter(x_2,y_2)

plot(xtheory,ytheory,label='Theoretical')

yerr_5 = [x / 2 for x in yerr_5]
yerr_1 = [x / 2 for x in yerr_1]
yerr_2 = [x / 2 for x in yerr_2]

errorbar(x_5,y_5,yerr_5, label='500MB', marker='o')
errorbar(x_1,y_1,yerr_1, label='1GB', marker='.')
errorbar(x_2,y_2,yerr_2, label='2GB', marker='^')


xlabel('# nodes')
ylabel('time (s)')
title('Load')
legend(loc='upper right')
show()
