import pandas as pd
import numpy as np


def main():
    file = "results/output_LinkFilteringHive"
    new = "results/linkFilteringTime.csv"
    outstat = "results/outstat_LinkFilteringHive_time.txt"
    ws = open(outstat, 'w')
    ws.write("Statistics for LinkFiltering using Hive" + '\n')
    w = open(new, 'w')
    w.write("testnum,time" + '\n')
    num = 0
    with open(file, 'r') as f:
        lines = f.readlines()
        for line in lines:
            num += 1
            time = line.split(":")[1].strip()
            print(num, time)
            w.write(str(num) + ',' + str(time) + '\n')

    w.close()

    data = pd.read_csv(new)

    mean = np.mean(data['time'])
    print("The mean is, ", mean)
    ws.write("The mean is, " + str(mean) + '\n')

    stdev = np.std(data['time'])
    print("The standard deviation is, ", stdev)
    ws.write("The standard deviation is, " + str(stdev) + '\n')

    ws.close()


if __name__ == '__main__':
    main()
