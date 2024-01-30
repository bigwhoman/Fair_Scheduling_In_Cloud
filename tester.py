import subprocess 
cores = [4, 8, 16, 32]
graphs = [4, 8, 16, 32]
task = [220, 500, 1000]
algorithms = ['MinMax', 'fdws', 'rank_hybd']


for core in cores :
    res = subprocess.run(['python', 'scheduler.py', 'GE', '2', '2', '2', 'MinMax'], capture_output=True, text=True)
    splited_res = res.stdout.split('\n')
    unfairness = 0
    makespan = 0
    for r in splited_res : 
        if 'unfairness' in r : 
            unfairness = float(r.split(' ')[2])
        if 'makespan' in r :
            makespan = float(r.split(' ')[2])
    print(unfairness, makespan)