import subprocess 
from pprint import pprint
import matplotlib.pyplot as plt

cores = [4, 8, 16, 32]
graphs = [4, 8, 16, 32]
tasks = [5, 10, 15]
algorithms = ['MinMax', 'fdws', 'rank_hybd']

for task in tasks:
    for metric in ['unfairness', 'makespan']:
        
        fig, axs = plt.subplots(1, 1)
        data = {alg: {} for alg in algorithms}
        for i, core in enumerate(cores):
            
            res = subprocess.run(['python', 'scheduler.py', 'GE', f'{task}', f'{core}', '8'], capture_output=True, text=True)
            splited_res = res.stdout.split('\n')
            
            
            for r in splited_res:
                if metric in r:
                    val = float(r.split(' ')[2])
                    alg = r.split(' ')[1]
                    data[alg][core] = val
        for alg, values in data.items():
            y = [values[i] for i in cores] 
            plt.plot(cores, y, label=alg)

        
        plt.xlabel("Cores")
        plt.legend()
        plt.ylabel(f"{metric}")
        
        plt.tight_layout()
        plt.savefig(f'{metric}_cores_task_{task}.png')

for task in tasks:
    for metric in ['unfairness', 'makespan']:
        
        fig, axs = plt.subplots(1, 1)
        data = {alg: {} for alg in algorithms}
        for i, graph in enumerate(graphs):
            
            res = subprocess.run(['python', 'scheduler.py', 'GE', f'{task}', '8', f'{graph}'], capture_output=True, text=True)
            splited_res = res.stdout.split('\n')
            
            
            for r in splited_res:
                if metric in r:
                    val = float(r.split(' ')[2])
                    alg = r.split(' ')[1]
                    data[alg][graph] = val
        for alg, values in data.items():
            y = [values[i] for i in graphs] 
            plt.plot(graphs, y, label=alg)

        
        plt.xlabel("Graphs")
        plt.legend()
        plt.ylabel(f"{metric}")
        
        plt.tight_layout()
        plt.savefig(f'{metric}_graphs_task_{task}.png')