import sys


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
    def FFT(self) -> str:
        pass 
    
    # Gaussian Elimination DAG generation
    def GE(self, m:int)  -> dict:
        all_graph_nodes = ( m * m + m - 2)/2
        all_nodes = []
        first_node = dict(self.base_task)
        for i in range(2,m-1 + 2) :
            child = dict(self.base_task)
            child["ID"] = i
            child["Fathers"].append(first_node)
            first_node["Children"].append(child)
        for i in range(2, all_graph_nodes + 1) :
            pass

if __name__ == "__main__" :
    if len(sys.argv) < 3 :
        print("Format shoud be as follows : method num_of_tasks")
        exit(1)
    generation_method = sys.argv[1]
    number_of_tasks = int(sys.argv[2])
    x = generate_tasks()


