import sys
from pprint import pprint
import math
import networkx as nx
import matplotlib.pyplot as plt
import random

class generate_tasks :
    base_task = {
        "ID" : 1, 
        "Fathers" : [],
        "Children" : [],
        "Computing_Time" : 1,
        "Communication_Cost" : 1
    }
    
    def __init__(self) :
        self = self 
    
    # Fast Fourier Transform DAG generation
    def FFT(self, m:int) -> dict:
        total_graph_nodes = int( m * math.log2(m) + 2 * m - 1)
        all_nodes = {}
        for node in range(1,total_graph_nodes + 1) :
            all_nodes[node] = {
                "ID" : node,
                "Fathers" : [],
                "Children" : [],
                "Computing_Time" : random.randint(10,100),
                "Communication_Cost" : random.randint(5,25)
            }
        
        for i in range(1,m-1):
            for j in range(2**(i-1), 2 ** (i)) :
                for k in range(j * 2, j * 2 + 2) :
                    all_nodes[j]["Children"].append(k)
                    all_nodes[k]["Fathers"].append(j)

        base_pointer = 2 * m
        forward      = -1
        for i in range(0, int(math.log2(m))) :
            for j in range(1, m + 1, 2 ** (i)) :
                forward *= -1
                for k in range(j,j + 2 ** (i)) :
                    all_nodes[base_pointer + k - 1]["Fathers"].append(base_pointer + k - m - 1)
                    all_nodes[base_pointer + k - 1]["Fathers"].append(base_pointer + k - m - 1 + forward * 2 ** (i))
                    all_nodes[base_pointer + k - m - 1 + forward * 2 ** (i)]["Children"].append(base_pointer + k - 1)
                    all_nodes[base_pointer + k - m - 1]["Children"].append(base_pointer + k - 1)
            base_pointer += m

        return all_nodes
    
    # Gaussian Elimination DAG generation
    def GE(self, m:int)  -> dict:
        total_graph_nodes = int(( m * m + m - 2)/2)
        all_nodes = {}
        for node in range(1,total_graph_nodes + 1) :
            all_nodes[node] = {
                "ID" : node,
                "Fathers" : [],
                "Children" : [],
                "Computing_Time" : random.randint(10,100),
                "Communication_Cost" : random.randint(5,25)
            }
            
        base_pointer = 1
        while m > 1 :
            for i in range(base_pointer + 1, base_pointer + m) :
                all_nodes[i]["Fathers"].append(base_pointer)
                all_nodes[base_pointer]["Children"].append(i)

            for i in range(base_pointer + 1, base_pointer + m ) :
                if i >= total_graph_nodes :
                    break
                all_nodes[i + m - 1]["Fathers"].append(i)
                all_nodes[i]["Children"].append(i + m - 1)           
            
            base_pointer += m
            m -= 1
        
        return all_nodes

    def draw_graph(self, graph:dict) :
        G = nx.DiGraph()

        for node, attrs in graph.items():
            G.add_node(node)
            for child in attrs['Children']:
                G.add_edge(node, child)

        pos = nx.drawing.nx_agraph.graphviz_layout(G, prog='dot')
        nx.draw(G, pos, with_labels=True, arrows=True)
        plt.show()

if __name__ == "__main__" :
    if len(sys.argv) < 3 :
        print("Format shoud be as follows : method + num_of_tasks")
        exit(1)
    generation_method = sys.argv[1]
    number_of_tasks = int(sys.argv[2])
    task_generator = generate_tasks()
    graph = None
    if generation_method == "GE" :
        graph = task_generator.GE(number_of_tasks)
    elif generation_method == "FFT" :
        graph = task_generator.FFT(number_of_tasks)
    else :
        print("Method should be : FFT - GE")
        exit(1)
    pprint(graph)
    task_generator.draw_graph(graph=graph)


