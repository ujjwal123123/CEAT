from __future__ import annotations

import csv
import sys
from collections import defaultdict
from typing import DefaultDict, List, Optional, Set, Tuple

TOTAL_TIME = 100000


class Core:
    def __init__(self, core_id: int):
        self.core_id = core_id

    def __eq__(self, o: object) -> bool:
        if not isinstance(o, Core):
            return False
        return self.core_id == o.core_id

    def __str__(self) -> str:
        return f"{self.core_id}"


class Task:
    def __init__(
        self, id: int, execution_requirement: float, period: int, rates: List[float]
    ):
        self.id = id
        self.period: int = period
        self.exec: float = execution_requirement  # execution requirement
        self.rates: List[float] = rates
        self.core_count = len(rates)
        self.__utilizations: Optional[List[float]] = None

    def __repr__(self) -> str:
        return f"{self.id}.\tPeriod: {self.period}\tExecution Requirement: {self.exec}"

    def get_utilizations(self) -> List[float]:
        if self.__utilizations is None:
            self.__utilizations = [self.exec * ratio for ratio in self.ratios]
        return self.__utilizations


class Cluster:
    def __init__(self, first_core: Core, second_core: Core):
        assert first_core != second_core
        if first_core.core_id < second_core.core_id:
            self.first_core = first_core
            self.second_core = second_core
        else:
            self.first_core = second_core
            self.second_core = first_core
        self.tasks_allocated: Set[Task] = set()

    def __eq__(self, o: object) -> bool:
        if not isinstance(o, Cluster):
            return False
        return (
            self.first_core == o.first_core and self.second_core == o.second_core
        ) or (self.second_core == o.first_core and self.first_core == o.second_core)

    def __contains__(self, core: Core) -> bool:
        return self.first_core == core or self.second_core == core

    def __hash__(self) -> int:
        return hash((self.first_core.core_id, self.second_core.core_id))


class FileReader:
    def __init__(self, file_name: str):
        self.file_name: str = file_name
        self.set_no: int
        self.cores_count: int
        self.global_utilisation: int
        self.set_no, self.cores_count, self.global_utilisation = self.__parse_file_name(
            file_name
        )
        self.tasks: List[Task] = self.__read_tasks(file_name)

    def __parse_row(self, row: List[str]) -> Task:
        id: int = int(row[0])
        exec: float = float(row[1])
        period: int = int(row[2])
        rates: List[float] = list(map(float, row[3:]))
        return Task(id, exec, period, rates)

    def __read_tasks(self, file_name: str) -> List[Task]:
        with open(file_name, "r") as inp_file:
            csv_reader = csv.reader(inp_file)
            next(csv_reader)  # skip the header row
            tasks: List[Task] = []
            for row in csv_reader:
                tasks.append(self.__parse_row(row))
            return tasks

    def __parse_file_name(self, file_name: str):
        """Extract set_no, cores_count and global_utilisation from the file name"""
        return [int(word) for word in file_name.split("_") if word.isdigit()]


class Scheduler:
    @staticmethod
    def task_with_lowest_requirement(
        tasks: List[Task], allocated_tasks: List[Task]
    ) -> Tuple[Task, Core]:
        """Returns the task with lowest execution requirement along with the core on
        which it should be executed"""
        lowest_requirement: Optional[float] = None
        task_lowest: Optional[Task] = None  # task with the lowest requirement
        core_lowest: Optional[
            Core
        ] = None  # core on which `task_lowest` should be executed

        for task in tasks:
            if task not in allocated_tasks:
                utilizations: List[float] = task.get_utilizations()
                if lowest_requirement is None or min(utilizations) < lowest_requirement:
                    lowest_requirement = min(utilizations)
                    task_lowest = task
                    core_lowest = Core(utilizations.index(lowest_requirement))

        assert task_lowest is not None
        assert core_lowest is not None

        return (task_lowest, core_lowest)

    @staticmethod
    def find_second_core(
        tasks: List[Task], task: Task, allocated_cores: List[Core]
    ) -> Core:
        """
        Find a core such that it does not belong to a cluster and share value is the
        next lowest value
        """
        lowest_value: float = min(task.get_utilizations())
        min_value: Optional[float] = None
        min_core: Optional[
            Core
        ] = None  # core associated with second lowest requirement value

        for core_index, requirement in enumerate(task.get_utilizations()):
            if Core(core_index) not in allocated_cores:
                if min_value is None or (lowest_value < requirement < min_value):
                    min_core = Core(core_index)
                    min_value = requirement

        assert min_core is not None
        assert min_core not in allocated_cores
        return min_core

    @staticmethod
    def find_cluster_with_core(clusters: Set[Cluster], core: Core) -> Cluster:
        for cluster in clusters:
            if core in cluster:
                return cluster

        raise Exception("Not found")

    def ceat(self, tasks: List[Task]):
        clusters: Set[Cluster] = self.construct_clusters(tasks)
        for index, cluster in enumerate(clusters):
            print(
                f"Cluster {index}: cores {cluster.first_core} and {cluster.second_core}"
            )
            for task in cluster.tasks_allocated:
                print(task)
            print()

    def construct_clusters(self, tasks: List[Task]) -> Set[Cluster]:
        tasks_allocated: List[Task] = []
        cores_allocated: List[Core] = []

        clusters: Set[Cluster] = set()

        while len(tasks_allocated) < len(tasks):
            task, first_core = Scheduler.task_with_lowest_requirement(
                tasks, tasks_allocated
            )

            if task is None or first_core is None:
                exit("Infeasible task set")

            if first_core not in cores_allocated:
                cores_allocated.append(first_core)
                second_core: Core = Scheduler.find_second_core(
                    tasks, task, cores_allocated
                )

                new_cluster = Cluster(first_core, second_core)
                new_cluster.tasks_allocated.add(task)

                cores_allocated.append(second_core)
                clusters.add(new_cluster)

            else:
                cluster: Cluster = Scheduler.find_cluster_with_core(
                    clusters, first_core
                )

                cluster.tasks_allocated.add(task)

            tasks_allocated.append(task)

        return clusters


if __name__ == "__main__":
    if len(sys.argv) == 1:
        print("Provide file name as an argument")
        exit()

    file_name = sys.argv[1]
    file = FileReader(file_name)
    scheduler = Scheduler()
    scheduler.ceat(file.tasks)
