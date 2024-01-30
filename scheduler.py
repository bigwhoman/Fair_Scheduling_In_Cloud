import copy
from heft import HEFT, ScheduledTask
from multi_schedulers import MinMax, DAG, FDWS_RANK_HYBRID
from task_generator import *
from pprint import pprint 
import csv

def calculate_slowdown(scheduledTasks : dict[dict[int, ScheduledTask]],
                        dags : dict[int, dict[int, Task]], 
                        cpus : int) -> [int, int]:
    print("---------------------------------------------------------------------------")
    pprint(scheduledTasks)
    start = 10000000000000000000000
    finish = 0
    unfairness = 0
    graph_things = {}
    for key in scheduledTasks.keys() :
        print("**********************************")
        graph_start = min(list(map(lambda x : scheduledTasks[key][x].start_time,
                                    scheduledTasks[key])))
        graph_end   = max(list(map(lambda x : scheduledTasks[key][x].computation_finish_time,
                                    scheduledTasks[key])))
        finish = max(finish, graph_end)
        start = min(start, graph_start)
        print(graph_start, graph_end)
        heft_sched = HEFT.schedule(dags[i], cpus)
        print(heft_sched)
        heft_start = min(list(map(lambda x : heft_sched[x].start_time, heft_sched)))
        heft_end   = max(list(map(lambda x : heft_sched[x].computation_finish_time, heft_sched)))
        M_Multi = graph_end - graph_start
        M_Own   = heft_end - heft_start
        slowdown = M_Own / M_Multi
        graph_things[key] = {'slowdown' : slowdown}
        print(M_Multi, M_Own)
    print("================================")
    average_slowdown = sum(list(
        map(lambda x : graph_things[x]['slowdown'],  graph_things)))/len(graph_things.keys())
    for key in graph_things.keys() :
        unfairness += abs(graph_things[key]['slowdown'] - average_slowdown)
    return (finish - start, unfairness)

if __name__ == "__main__":
    if len(sys.argv) < 5:
        print("Format should be as follows : grath generation method + num_of_tasks + cpu cores + num_of_graphs")
        exit(1)
    generation_method = sys.argv[1]
    number_of_tasks = int(sys.argv[2])
    cpu_cores = int(sys.argv[3])
    num_of_graphs = int(sys.argv[4])
    # scheduling_method = sys.argv[5]
    dags = dict()
    dags_copy = dict()
    task_generator = generate_tasks()
    for i in range(num_of_graphs) :
        if generation_method == "GE":
            graph = task_generator.GE(number_of_tasks)
        elif generation_method == "FFT":
            graph = task_generator.FFT(number_of_tasks)
        else:
            print("Method should be : FFT - GE")
            exit(1)
        print(f"dag {i} Currently has", len(graph), "tasks")
        for task in graph.values():
            task.populate_cpu_dependant_variables(cpu_cores)
        dags_copy[i] = copy.deepcopy(graph)
        dags[i] = DAG(i,graph,cpu_cores)
    makespan = 0
    unfairness = 0
    makespan, unfairness = calculate_slowdown(MinMax.schedule(dags=copy.deepcopy(dags), cpus=cpu_cores),
                                                   copy.deepcopy(dags_copy), cpu_cores)
    print(f"makespan MinMax {makespan}")
    print(f"unfairness MinMax {unfairness}")
    makespan, unfairness = calculate_slowdown(FDWS_RANK_HYBRID
                                                  .schedule(dags=copy.deepcopy(dags), cpus=cpu_cores, is_fdws=True),
                                                    copy.deepcopy(dags_copy), cpu_cores)
    print(f"makespan fdws {makespan}")
    print(f"unfairness fdws {unfairness}")
    makespan, unfairness = calculate_slowdown(FDWS_RANK_HYBRID
                                                  .schedule(dags=copy.deepcopy(dags), cpus=cpu_cores, is_fdws=False),
                                                   copy.deepcopy(dags_copy), cpu_cores)
    print(f"makespan rank_hybd {makespan}")
    print(f"unfairness rank_hybd {unfairness}")

    

