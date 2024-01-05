import sys
from task_generator import Task, generate_tasks


class HEFT:
    @staticmethod
    def rank(tasks: dict[int, Task]) -> list[tuple[float, Task]]:
        # task-id -> rank
        task_ranks: dict[int, float] = {}
        for id in range(1, len(tasks) + 1):
            if id in task_ranks:
                continue
            task_ranks[id] = HEFT.calculate_rank(tasks, task_ranks, id)
        result: list[tuple[float, Task]] = []
        for id, rank in sorted(task_ranks.items(), key=lambda item: item[1]):
            result.append((rank, tasks[id]))
        return result

    @staticmethod
    def calculate_rank(
        tasks: dict[int, Task], task_ranks: dict[int, float], wanted: int
    ) -> float:
        if wanted in task_ranks:
            return task_ranks[wanted]
        current_task = tasks[wanted]
        if len(current_task.children) == 0:
            return current_task.average_computation()
        current_rank = current_task.average_computation() + max(
            map(
                lambda child_task_id: current_task.average_communication(child_task_id)
                + HEFT.calculate_rank(tasks, task_ranks, child_task_id),
                current_task.children,
            )
        )
        task_ranks[wanted] = current_rank
        print(f"{wanted} -> {current_rank}")
        return current_rank

    @staticmethod
    def schedule(tasks: dict[int, Task]):
        pass


if __name__ == "__main__":
    if len(sys.argv) < 4:
        print("Format should be as follows : method + num_of_tasks + cpu cores")
        exit(1)
    generation_method = sys.argv[1]
    number_of_tasks = int(sys.argv[2])
    cpu_cores = int(sys.argv[3])
    task_generator = generate_tasks()
    if generation_method == "GE":
        graph = task_generator.GE(number_of_tasks)
    elif generation_method == "FFT":
        graph = task_generator.FFT(number_of_tasks)
    else:
        print("Method should be : FFT - GE")
        exit(1)
    print("Currently have", len(graph), "tasks")
    for task in graph.values():
        task.populate_cpu_dependant_variables(cpu_cores)
    print(HEFT.rank(graph))
