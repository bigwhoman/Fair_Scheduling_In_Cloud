import sys
from pprint import pprint
import math
import networkx as nx
import matplotlib.pyplot as plt
import random


class Task:
    def __init__(self, id: int):
        self.id = id
        self.fathers: list[int] = []
        self.children: list[int] = []
        self.average_computation_time = random.randint(10, 100)
        self.real_computation_time = min(
            max(1, int(random.normalvariate(self.average_computation_time, 5))),
            self.average_computation_time + 25,
        )
        # This should be used like this: First index is the ID of the task which we want to comminate with (the child ID)
        # The second index is the core ID which this task is currently on
        # The third index is the core ID which child task should run on
        self.communication_cost: dict[int, list[list[int]]] = []

    def generate_random_communication_cost(self, cpu_count: int):
        self.communication_cost.clear()
        for child_id in range(self.children):
            costs = [[0] * cpu_count for _ in range(cpu_count)]
            for cpu1 in range(cpu_count):
                for cpu2 in range(cpu_count):
                    if cpu1 == cpu2:
                        continue
                    costs[cpu1][cpu2] = random.randint(5, 25)
            self.communication_cost[child_id] = costs


class generate_tasks:
    # Fast Fourier Transform DAG generation
    @staticmethod
    def FFT(m: int) -> dict[int, Task]:
        total_graph_nodes = int(m * math.log2(m) + 2 * m - 1)
        all_nodes: dict[int, Task] = {}
        for node in range(1, total_graph_nodes + 1):
            all_nodes[node] = Task(node)

        for i in range(1, m - 1):
            for j in range(2 ** (i - 1), 2 ** (i)):
                for k in range(j * 2, j * 2 + 2):
                    all_nodes[j].children.append(k)
                    all_nodes[k].fathers.append(j)

        base_pointer = 2 * m
        forward = -1
        for i in range(0, int(math.log2(m))):
            for j in range(1, m + 1, 2 ** (i)):
                forward *= -1
                for k in range(j, j + 2 ** (i)):
                    all_nodes[base_pointer + k - 1].fathers.append(
                        base_pointer + k - m - 1
                    )
                    all_nodes[base_pointer + k - 1].fathers.append(
                        base_pointer + k - m - 1 + forward * 2 ** (i)
                    )
                    all_nodes[
                        base_pointer + k - m - 1 + forward * 2 ** (i)
                    ].children.append(base_pointer + k - 1)
                    all_nodes[base_pointer + k - m - 1].children.append(
                        base_pointer + k - 1
                    )
            base_pointer += m

        return all_nodes

    # Gaussian Elimination DAG generation
    @staticmethod
    def GE(m: int) -> dict[int, Task]:
        total_graph_nodes = int((m * m + m - 2) / 2)
        all_nodes: dict[int, Task] = {}
        for node in range(1, total_graph_nodes + 1):
            all_nodes[node] = Task(node)

        base_pointer = 1
        while m > 1:
            for i in range(base_pointer + 1, base_pointer + m):
                all_nodes[i].fathers.append(base_pointer)
                all_nodes[base_pointer].children.append(i)

            for i in range(base_pointer + 1, base_pointer + m):
                if i >= total_graph_nodes:
                    break
                all_nodes[i + m - 1].fathers.append(i)
                all_nodes[i].children.append(i + m - 1)

            base_pointer += m
            m -= 1

        return all_nodes

    @staticmethod
    def draw_graph(graph: dict[int, Task]):
        G = nx.DiGraph()

        for node, attrs in graph.items():
            G.add_node(node)
            for child in attrs.children:
                G.add_edge(node, child)

        pos = nx.drawing.nx_agraph.graphviz_layout(G, prog="dot")
        nx.draw(G, pos, with_labels=True, arrows=True)
        plt.show()


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Format should be as follows : method + num_of_tasks")
        exit(1)
    generation_method = sys.argv[1]
    number_of_tasks = int(sys.argv[2])
    task_generator = generate_tasks()
    graph = None
    if generation_method == "GE":
        graph = task_generator.GE(number_of_tasks)
    elif generation_method == "FFT":
        graph = task_generator.FFT(number_of_tasks)
    else:
        print("Method should be : FFT - GE")
        exit(1)
    pprint(graph)
    task_generator.draw_graph(graph=graph)
