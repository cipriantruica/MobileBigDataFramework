import os
import pickle
import time
import datetime
from scipy.integrate import quad
import numpy as np
import networkx as nx


def filtering(conn_matrix, N, alfa):
    # Strong and weak ties

    strong_ties = {}
    thr = alfa
    connectivity = conn_matrix
    for i in range(N):
        node_strength = connectivity[i, :].sum() - connectivity[i, i]
        # node_strength = connectivity[i,:].sum()
        for j in range(N):
            weight = connectivity[i, j] / node_strength
            if np.isnan(weight):
                # print "nan value!"
                continue
            else:
                alpha = 1 - (N - 2) * quad(integrand, 0, weight)[0]
                # quad is general purpose integration
                # integrand is integral function where 0 is lowest border and weight is highest
                # fw.write(str(i + 1) + ',' + str(j + 1) + ',' + str(weight) + '\n')
                # alpha = 1- (N-1) * quad(integrand, 0, weight)[0]
                if alpha < thr:
                    # print quad(integrand, 0, weight)[0], quad(integrand, 0, weight)[1]
                    strong_ties[i + 1, j + 1] = connectivity[i, j]
                    strong_ties[j + 1, i + 1] = connectivity[j, i]

    print_new_graph(strong_ties, thr)


def print_new_graph(graph, thr):
    alfa_name = str(thr)
    G = nx.Graph()  # pravi graph
    new_graph = "edgelistfilter_" + alfa_name + ".txt"
    wf = open(new_graph, 'w')

    keys = graph.keys()
    # print keys

    for key in keys:
        G.add_edge(key[0], key[1], weight=graph[key])
        if graph[key] is not None:
            wf.write(str(key[0]) + ', ' + str(key[1]) + ', ' + str(graph[key]) + '\n')

    # picke dump sacuvati graph
    gfile = "new_graph_" + alfa_name + ".txt"
    pickle.dump(G, open(gfile, 'w'))
    wf.close()


# ovde pravim connectivity matricu od grapha koji je generisao Spark
def connectivity_matrix(graph_file):
    beg_ts = time.time()
    st = datetime.datetime.fromtimestamp(beg_ts).strftime('%Y-%m-%d %H:%M:%S')
    print "Begin time of creating connectivity matrix:   ", st

    m, n = 10001, 10001
    connectivity = np.zeros((m, n))
    with open(graph_file, 'r') as g:
        lines = g.readlines()
        for line in lines:
            el = line.split(',')
            sqid1 = int(el[0][1:])
            sqid2 = int(el[1])
            w = float(el[2][: -2])
            connectivity[sqid1, sqid2] = w

    end_ts = time.time()
    st = datetime.datetime.fromtimestamp(end_ts).strftime('%Y-%m-%d %H:%M:%S')
    print "End time of creating connectivity matrix:   ", st

    return connectivity


N = 10000


def integrand(x):
    # return (1 - x) ** (N - 3)
    return np.power((1 - x), (N - 3))


def main():
    days = [1, 2, 3]
    for d in days:
        path = "/media/olivera/storage/EdgeListsNovember/myedgelist_" + str(d) + "/"
        print "This is day ", d
        graph = path + "part-00000"
        os.chdir(path)

        connectivity = connectivity_matrix(graph)

        N = len(connectivity)
        print "ovo je N ", N

        alfa_value = [0.01, 0.05, 0.001]

        for a in alfa_value:
            print "alfa value is: ", a

            beg_ts = time.time()
            st = datetime.datetime.fromtimestamp(beg_ts).strftime('%Y-%m-%d %H:%M:%S')
            print "Begin time of filtering:   ", st

            filtering(connectivity, N, a)

            end_ts = time.time()
            st = datetime.datetime.fromtimestamp(end_ts).strftime('%Y-%m-%d %H:%M:%S')
            print "End time of filtering:   ", st


if __name__ == '__main__':
    main()
