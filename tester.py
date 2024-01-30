import subprocess 
from pprint import pprint

cores = [4]
graphs = [4, 8, 16, 32]
task = [21, 32, 45]
algorithms = ['MinMax', 'fdws', 'rank_hybd']


for core in cores :
    res = subprocess.run(['python', 'scheduler.py', 'GE', '40', '2', '2', 'MinMax'], capture_output=True, text=True)
    splited_res = res.stdout.split('\n')
    # pprint(splited_res)
    unfairness = 0
    makespan = 0
    tester = { x : {} for x in algorithms  }
    for r in splited_res : 
        if 'unfairness' in r : 
            unfairness = float(r.split(' ')[2])
            tester[r.split(' ')[1]]['unfairness'] = unfairness
        if 'makespan' in r :
            makespan = float(r.split(' ')[2])
            tester[r.split(' ')[1]]['makespan'] = makespan
    pprint(tester)