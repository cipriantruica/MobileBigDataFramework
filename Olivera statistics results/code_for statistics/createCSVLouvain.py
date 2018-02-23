import pandas as pd
import numpy as np
import os


def main():
    currentpath = os.getcwd()
    path = currentpath + "/results/runtimeLouvain"
    os.chdir(path)
    outfile = currentpath + "/results/runtimeAlldays.csv"
    wout = open(outfile, 'w')
    wout.write("date,test_num,time" + '\n')

    for filename in os.listdir(path):
        date = filename[12:22]

        with open(filename, 'r') as f:
            lines = f.readlines()
            test_num = lines[0][27:29]
            time = lines[3].split(":")[1].strip()
            wout.write(date + ',' + str(test_num) + ',' + str(time) + '\n')

    wout.close()


if __name__ == '__main__':
    main()
