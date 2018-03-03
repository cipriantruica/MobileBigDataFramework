import csv
import sys
import evaluation_measures

filename = sys.argv[1]

Community1toID = {}
IDtoCommunity1 = {}
Community2toID = {}
IDtoCommunity2 = {}

no_nodes = {}

idx_c1 = 0
idx_c2 = 0

cluster_dic = {}
csv_data = []
if __name__ == '__main__':
    with open(filename) as csvfile:
        print(filename)
        reader = csv.DictReader(csvfile)
        for row in reader:
            if Community1toID.get(int(row['Community1'])) == None:
                Community1toID[int(row['Community1'])] = idx_c1
                IDtoCommunity1[idx_c1] = int(row['Community1'])
                idx_c1 += 1
            if Community2toID.get(int(row['Community2'])) == None:
                Community2toID[int(row['Community2'])] = idx_c2
                IDtoCommunity2[idx_c2] = int(row['Community2'])
                idx_c2 += 1
            # print(row['Community1'], Community1toID[int(row['Community1'])], row['Community2'], Community2toID[int(row['Community2'])])
            csv_data.append(row)

        # create a dictionary with 0
        for id_c1 in IDtoCommunity1:
            line = {}
            for id_c2 in IDtoCommunity2:
                line[id_c2] = 0
            cluster_dic[id_c1] = line
        
        for row in csv_data:        
            cluster_dic[Community1toID[int(row['Community1'])]][Community2toID[int(row['Community2'])]] = int(row['Nodes'])
    
        print('Purity:  ', evaluation_measures.entropy(cluster_dic))
        print('Entropy: ', evaluation_measures.purity(cluster_dic))
        print('RI       ', evaluation_measures.rand_index(cluster_dic))
        print('ARI      ', evaluation_measures.adj_rand_index(cluster_dic))