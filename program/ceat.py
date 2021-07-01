from __future__ import annotations

import csv
import sys
from collections import defaultdict
from typing import DefaultDict, List, Optional, Set, Tuple

TOTAL_TIME = 10000


class Core:
    def __init__(self, core_id: int):
        self.core_id = core_id

    def __eq__(self, o: object) -> bool:
        if not isinstance(o, Core):
            return False
        return self.core_id == o.core_id

    def __str__(self) -> str:
        return f"{self.core_id}"

    def __hash__(self) -> int:
        return hash(self.core_id)


class Task:
    def __init__(
        self, id: int, execution_requirement: float, period: int, rates: List[float]
    ):
        self.id = id
        self.period: int = period
        self.remaining_period: float = period
        self.exec: float = execution_requirement  # execution requirement
        self.rates: List[float] = rates
        self.core_count = len(rates)

    def __repr__(self) -> str:
        return f"{self.id}.\tPeriod: {self.period}\tExecution Requirement: {self.exec}"

    def get_utilizations(self) -> List[float]:
        return [self.exec / (self.period * rate) for rate in self.rates]

    def get_utilization(self, core: Core) -> float:
        return self.get_utilizations()[core.core_id]

    def get_share(self, core: Core, frame_length: float):
        return (self.exec * frame_length) / (self.period * self.rates[core.core_id])


class Cluster:
    def __init__(self, first_core: Core, second_core: Core, frame_length: float):
        assert first_core != second_core
        if first_core.core_id < second_core.core_id:
            self.first_core = first_core
            self.second_core = second_core
        else:
            self.first_core = second_core
            self.second_core = first_core
        self.spare_capacity: Optional[float]
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

    def set_spare_capacity(self, spare_capacity: float):
        self.spare_capacity = spare_capacity


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
    def task_with_lowest_share(
        tasks: List[Task],
        allocated_tasks: List[Task],
        task_core_considered: DefaultDict[Task, Set[Core]],
    ) -> Optional[Tuple[Task, Core]]:
        """Returns the task with lowest share value along with the core on
        which it should be executed"""
        lowest_requirement: Optional[float] = None
        task_lowest: Optional[Task] = None  # task with the lowest requirement
        core_lowest: Optional[
            Core
        ] = None  # core on which `task_lowest` should be executed

        for task in tasks:
            if task not in allocated_tasks:
                for core_id in range(task.core_count):
                    core = Core(core_id)

                    if core in task_core_considered[task]:
                        continue
                    utilization: float = task.get_utilization(core)
                    if lowest_requirement is None or utilization < lowest_requirement:
                        lowest_requirement = utilization
                        task_lowest = task
                        core_lowest = core

        if core_lowest:
            assert task_lowest is not None
            return (task_lowest, core_lowest)
        else:
            print("Task could not be assigned")
            return None

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
        ] = None  # core associated with second lowest utilization

        for core_index, requirement in enumerate(task.get_utilizations()):
            core: Core = Core(core_index)
            if core not in allocated_cores:
                if min_value is None or (lowest_value < requirement < min_value):
                    min_core = core
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

    @staticmethod
    def average_computation_demand(
        task: Task, first_core: Core, second_core: Core, frame_length: float
    ) -> float:
        # TODO: must I use get_share() here?
        return (
            task.get_share(first_core, frame_length)
            + task.get_share(second_core, frame_length)
        ) / 2

    def ceat(self, tasks: List[Task]):
        time = 0
        while time < TOTAL_TIME:
            print("running")
            frame_length: float = min([task.remaining_period for task in tasks])
            for task in tasks:
                if task.remaining_period == 0:
                    task.remaining_period = task.period
            clusters: Set[Cluster] = self.construct_clusters(tasks, frame_length)

            for index, cluster in enumerate(clusters):
                print(
                    f"Cluster {index}: cores {cluster.first_core} and {cluster.second_core}"
                )
                for task in cluster.tasks_allocated:
                    print(task)
                print()

            for task in tasks:
                task.remaining_period -= frame_length
            time += frame_length

    def construct_clusters(
        self, tasks: List[Task], frame_length: float
    ) -> Set[Cluster]:
        tasks_allocated: List[Task] = []
        cores_allocated: List[Core] = []

        clusters: Set[Cluster] = set()
        for cluster in clusters:
            cluster.set_spare_capacity(2 * frame_length)

        tasks_cores_considered: DefaultDict[Task, Set[Core]] = defaultdict(set)

        while len(tasks_allocated) < len(tasks):
            ret = Scheduler.task_with_lowest_share(
                tasks, tasks_allocated, tasks_cores_considered
            )
            if ret is None:
                exit("Infeasible task set")
            task, first_core = ret

            tasks_cores_considered[task].add(first_core)

            if first_core not in cores_allocated:
                cores_allocated.append(first_core)
                second_core: Core = Scheduler.find_second_core(
                    tasks, task, cores_allocated
                )
                computation_demand = Scheduler.average_computation_demand(
                    task, first_core, second_core, frame_length
                )
                tasks_cores_considered[task].add(second_core)
                if computation_demand < frame_length:
                    new_cluster = Cluster(first_core, second_core, frame_length)
                    new_cluster.set_spare_capacity(2 * frame_length)
                    new_cluster.tasks_allocated.add(task)

                    cores_allocated.append(second_core)
                    clusters.add(new_cluster)
                    tasks_allocated.append(task)

                    assert new_cluster.spare_capacity is not None
                    new_cluster.spare_capacity -= computation_demand

            else:
                cluster: Cluster = Scheduler.find_cluster_with_core(
                    clusters, first_core
                )

                tasks_cores_considered[task].add(cluster.first_core)
                tasks_cores_considered[task].add(cluster.second_core)

                computation_demand: float = Scheduler.average_computation_demand(
                    task, cluster.first_core, cluster.second_core, frame_length
                )
                # TODO: following is the buggy line
                assert cluster.spare_capacity is not None
                if computation_demand <= cluster.spare_capacity + 100:
                    cluster.tasks_allocated.add(task)
                    tasks_allocated.append(task)
                    cluster.spare_capacity -= computation_demand

        return clusters


if __name__ == "__main__":
    if len(sys.argv) == 1:
        print("Provide file name as an argument")
        exit()

    file_name = sys.argv[1]
    file = FileReader(file_name)
    scheduler = Scheduler()
    scheduler.ceat(file.tasks)
