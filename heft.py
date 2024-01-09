import json
import sys
from task_generator import Task, generate_tasks
from pprint import pprint
import matplotlib.pyplot as plt
import numpy as np

class ScheduledTask:
    def __init__(self, task: Task, cpu_id: int, start_time: int):
        self.task_id = task.id
        self.start_time = start_time
        self.computation_finish_time = start_time + task.computation_times[cpu_id]
        self.ran_cpu_id = cpu_id

    def __repr__(self) -> str:
        return str(
            {
                "id": self.task_id,
                "start": self.start_time,
                "end": self.computation_finish_time,
                "cpu_id": self.ran_cpu_id,
            }
        )


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
        for id, rank in sorted(
            task_ranks.items(), key=lambda item: item[1], reverse=True
        ):
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
    def is_interval_occupied_in_time(
        intervals: list[tuple[int, int]], interval: tuple[int, int]
    ) -> bool:
        # https://stackoverflow.com/a/3269471/4213397
        return any(
            map(lambda i: i[0] <= interval[1] and interval[0] <= i[1], intervals)
        )

    @staticmethod
    def find_gap(
        scheduled_tasks: dict[int, ScheduledTask],
        cpu_id: int,
        fastest_start_time: int,
        computation_cost: int,
    ) -> int:
        """
        Finds the first time which we can schedule a task on a specific core
        """
        occupied_intervals = sorted(
            map(
                lambda task: (task.start_time, task.computation_finish_time),
                filter(
                    lambda item: item.ran_cpu_id == cpu_id, scheduled_tasks.values()
                ),
            ),
            key=lambda item: item[0],  # sort by start time
        )
        candidate_start_time = fastest_start_time
        # This is a horrible way to do it but whatever
        while HEFT.is_interval_occupied_in_time(
            occupied_intervals,
            (candidate_start_time, candidate_start_time + computation_cost),
        ):
            candidate_start_time += 1
        return candidate_start_time

    @staticmethod
    def schedule(tasks: dict[int, Task], cpus: int) -> dict[int, ScheduledTask]:
        ranks = HEFT.rank(tasks)
        # task_id -> task
        scheduled_tasks: dict[int, ScheduledTask] = {}
        for task in map(lambda rank: rank[1], ranks):
            # cpu_id -> (start, finish) for each CPU core
            cpu_runtimes: list[tuple[int, int]] = [(0, 0)] * cpus
            for cpu_id in range(cpus):
                processor_ready = 0  # when does this core can become available for scheduling this task
                for parent_id in task.fathers:
                    assert parent_id in scheduled_tasks  # sanity check
                    communication_cost = tasks[parent_id].communication_cost[task.id][
                        scheduled_tasks[parent_id].ran_cpu_id
                    ][cpu_id]
                    start_delay = (
                        communication_cost
                        + scheduled_tasks[parent_id].computation_finish_time
                    )
                    processor_ready = max(processor_ready, start_delay)
                # Now calculate when we can schedule this task
                cpu_start_time = HEFT.find_gap(
                    scheduled_tasks,
                    cpu_id,
                    processor_ready,
                    task.computation_times[cpu_id],
                )
                cpu_runtimes[cpu_id] = (
                    cpu_start_time,
                    cpu_start_time + task.computation_times[cpu_id],
                )
            # Now check what CPU yields the fastest one
            best_cpu_id = min(
                range(len(cpu_runtimes)), key=lambda x: cpu_runtimes[x][1]
            )
            scheduled_tasks[task.id] = ScheduledTask(
                task, best_cpu_id, cpu_runtimes[best_cpu_id][0]
            )
        return scheduled_tasks
    
    @staticmethod
    def draw_chart(tasks : dict[int, ScheduledTask]):
        colors = np.random.rand(len(tasks), 3)
        all_schedules = [[[task.start_time, task.computation_finish_time]] for task in tasks.values()]

        max_end = max([max(sched[-1][-1] for sched in all_schedules)])

        fig, ax = plt.subplots(1, figsize=(8, 4))
        for i, task in enumerate(tasks):

            schedules = [[tasks[task].start_time, tasks[task].computation_finish_time]]
            for start, end in schedules:
                ax.broken_barh([(start, end-start)],
                            (i-0.4, 0.8), facecolors=colors[i])
                ax.annotate(tasks[task].ran_cpu_id, xy=(start + (end-start)/2, i-0.2), ha='center')
            ax.text(-0.1, i, task, ha='right', va='center')

        ax.set_xlim(0, max_end)
        # ax.set_xticks(np.arange(0, max_end + 1, 1))
        ax.set_xlabel('Time')
        ax.set_ylabel('Tasks')
        ax.set_yticklabels([])
        ax.set_title('Gantt Chart')

        plt.show()


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
    print("Ranked tasks:")
    pprint(HEFT.rank(graph))
    sched = HEFT.schedule(graph, cpu_cores)
    pprint(sched)
    print("Makespan:", max(map(lambda task: task.computation_finish_time, sched.values())))
    HEFT.draw_chart(sched)
