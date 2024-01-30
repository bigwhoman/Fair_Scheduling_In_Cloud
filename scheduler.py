from heft import HEFT
from multi_schedulers import MinMax, DAG
from task_generator import *
from pprint import pprint 



if __name__ == "__main__":
    if len(sys.argv) < 5:
        print("Format should be as follows : method + num_of_tasks + cpu cores + num_of_graphs")
        exit(1)
    generation_method = sys.argv[1]
    number_of_tasks = int(sys.argv[2])
    cpu_cores = int(sys.argv[3])
    num_of_graphs = int(sys.argv[4])
    dags = dict()
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
        dags[i] = DAG(i,graph,cpu_cores)
    pprint(dags[0].tasks)
    pprint(MinMax.schedule(dags=dags, cpus=cpu_cores))
    # print("Ranked tasks:")
    # pprint(HEFT.rank(graph))
    # sched = HEFT.schedule(graph, cpu_cores)
    # pprint(sched)
    # print("Makespan:", max(map(lambda task: task.computation_finish_time, sched.values())))
    # HEFT.draw_chart(sched)


