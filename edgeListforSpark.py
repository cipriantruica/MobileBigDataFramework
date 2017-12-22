import os


def make_edge_list(input, filter):
    new_file_name = "edgelistforspark_" + str(filter) + ".txt"

    new = open(new_file_name, 'w')

    with open(input, 'r') as e:
        lines = e.readlines()
        for line in lines:
            el = line.split(',')
            sqid1 = el[0]
            sqid2 = el[1]
            #w = el[2]
            new.write(sqid1 + '\t' + sqid2 + '\n')

    new.close()


def main():
    for i in range(1,4):
        path = "/home/olivera/edge_lists_filtered/myedgelist_" + str(i)
        print "day ", i
        os.chdir(path)
        alfa = [0.01, 0.05, 0.001]
        for a in alfa:
            input = "edgelistfilter_" + str(a) + ".txt"
            make_edge_list(input, a)


if __name__ == '__main__':
    main()
