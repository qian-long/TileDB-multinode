from pylab import *
x_5 = [1,2,4,8]
x_1 = [2,4,8]
x_2 = [4,8]
##load 
#500
y_5 = [0.300585, 0.1523733333 ,   0.1031513333 ,   0.06773333333]
yerr_5 = [0.006655,  0.006  , 0.074226  ,  0.01554]
#1
y_1 = [0.2084566667, 0.1602666667 ,   0.1315333333]
yerr_1 = [0.27195 ,  0.0212,  0.0404]

#2
y_2 = [0.3501 ,  0.2405333333]
yerr_2 = [0.4538 ,   0.1568]

xtheory = [1,2,4,8]
ytheory = [.4, .2, .1, .05]

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
title('Aggregate')
legend(loc='upper right')
show()
