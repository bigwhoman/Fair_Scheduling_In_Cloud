import sys
from pprint import pprint
import math

class generate_tasks :
    base_task = {
        "ID" : 1, 
        "Fathers" : [],
        "Children" : [],
        "Computing_Time" : 1
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
                "Computing_Time" : 1
            }
        
        for i in range(1,m-1):
            for j in range(2**(i-1), 2 ** (i)) :
                for k in range(j * 2, j * 2 + 2) :
                    all_nodes[j]["Children"].append(k)
                    all_nodes[k]["Fathers"].append(j)

        for i in range(0, int(math.log2(m))) :
            pass

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
                "Computing_Time" : 1
            }
            
        base_pointer = 1
        while m > 1 :
            for i in range(base_pointer + 1, base_pointer + m) :
                all_nodes[i]["Fathers"].append(base_pointer)
                all_nodes[base_pointer]["Children"].append(i)

            for i in range(base_pointer + 2, base_pointer + m ) :
                all_nodes[i + m - 1]["Fathers"].append(i)
                all_nodes[i]["Children"].append(i + m - 1)           
            
            base_pointer += m
            m -= 1
        
        return all_nodes

if __name__ == "__main__" :
    # if len(sys.argv) < 3 :
    #     print("Format shoud be as follows : method num_of_tasks")
    #     exit(1)
    # generation_method = sys.argv[1]
    # number_of_tasks = int(sys.argv[2])
    number_of_tasks = 4
    task_generator = generate_tasks()
    pprint(task_generator.FFT(number_of_tasks))


