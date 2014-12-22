from pylab import *
x_5 = [1,2,4,8]
x_1 = [2,4,8]
x_2 = [4,8]
##load 
#500
y_5 = [7.033366667,  3.939366667, 2.31558, 1.59569]
yerr_5 = [0.4032,    0.1781,  0.915,   0.27393]
#1
y_1 = [7.032733333,  4.942266667, 3.303766667]
yerr_1 = [0.183,    1.323,   0.789]

#2
y_2 = [8.274666667,  5.381]
yerr_2 = [0.0263,    0.458]

xtheory = [1,2,4,8]
ytheory = [8, 4, 2, 1]

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
title('Filter')
legend(loc='upper right')
show()
