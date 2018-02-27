import pandas as pd
import numpy as np
import os


def main():
    print(os.getcwd())
    ws = open("LouvainRuntimeStat.txt", 'w')
    ws.write("Statistics for Louvain runtime 10 test" + '\n' + '\n')
    data = pd.read_csv("runtimeAlldays.csv")
    df = pd.DataFrame(data)

    dd = ['2013-11-01', '2013-11-02', '2013-11-03', '2013-11-04', '2013-11-05',
          '2013-11-06', '2013-11-07', '2013-11-08', '2013-11-09', '2013-11-10',
          '2013-11-11', '2013-11-12', '2013-11-13', '2013-11-14', '2013-11-15',
          '2013-11-16', '2013-11-17', '2013-11-18', '2013-11-19', '2013-11-20',
          '2013-11-21', '2013-11-22', '2013-11-23', '2013-11-24', '2013-11-25',
          '2013-11-26', '2013-11-27', '2013-11-28', '2013-11-29', '2013-11-30']

    for d in dd:
        print(d)
        seldf = df.loc[df['date'] == d]
        mean = np.mean(seldf['time'])
        stdev = np.std(seldf['time'])
        ws.write("date: " + str(d) + '\n'
                 + "The mean is, " + str(mean) + '\n'
                 + "The standard deviation is, " + str(stdev) + '\n' + '\n')


    #mean = np.mean(data['time'])
    #print("The mean is, ", mean)
    #ws.write("The mean is, " + str(mean) + '\n')

    #stdev = np.std(data['time'])
    #print("The standard deviation is, ", stdev)
    #ws.write("The standard deviation is, " + str(stdev) + '\n')

    ws.close()


if __name__ == '__main__':
    main()
