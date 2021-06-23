import csv
from typing import List, Mapping, Optional, Set, Tuple, Dict

TOTAL_TIME = 100000
NO_OF_CORES = 2


class Core(object):
    core_mapping: Dict[int, "Core"] = dict()

    def __init__(self, id: int):
        self.id: int = id
        self.tasks: List[Task] = []

        assert id not in Core.core_mapping.keys()

        Core.core_mapping[id] = self

    def __hash__(self) -> int:
        return hash(self.id)


class Cluster:
    def __init__(self, core1: Core, core2: Core, current_frame: float):
        assert core1 != core2
        self.core1 = core1
        self.core2 = core2
        self.tasks: Set[Task] = set()
        self.spare_capacity = 2 * current_frame

    def __eq__(self, o: "Cluster") -> bool:
        return (self.core1 == o.core1 and self.core2 == o.core2) and (
            self.core2 == o.core1 and self.core1 == o.core2
        )

    def __contains__(self, item: Core):
        return self.core1 is item or self.core2 is item

    def __hash__(self) -> int:
        return hash((self.core1, self.core2))

    def cores(self):
        return [self.core1, self.core2]


class Task:
    def __init__(self, id: int, exec1: int, exec2: int, period: int):
        self.id = id
        self.period = period
        self.execs = [exec1, exec2]  # execution requirements

        self.utilizations: Optional[List[float]] = None
        self.shares: Optional[List[float]] = None

    def __str__(self) -> str:
        return (
            f"{self.id}.\tPeriod: {self.period}\t Execution requirement: {self.execs}"
        )

    def get_utilizations(self) -> List[float]:
        if self.utilizations is None:
            self.utilizations = [exec / self.period for exec in self.execs]
        return self.utilizations

    def get_shares(self, frame_size: float):
        if self.shares is None:
            self.shares = [
                utilization * frame_size for utilization in self.get_utilizations()
            ]
        return self.shares

    def invalidate_data(self):
        self.utilizations = None
        self.shares = None


class FileInput:
    def __init__(self, file_name: str) -> None:
        self.file_name: str = file_name
        self.set_no: Optional[int] = None
        self.cores_count: Optional[int] = None
        self.utilisation: Optional[int] = None
        self.tasks: List[Task] = self.__set_tasks()

        self.__set_data_properties()

    def __set_tasks(self) -> List[Task]:
        with open(self.file_name, "r") as inp_file:
            read_file = csv.reader(inp_file, delimiter="\t")
            tasks = [Task(*map(int, row[:4])) for row in read_file]
            return tasks

    def __set_data_properties(self):
        integers = [int(word) for word in self.file_name.split("_") if word.isdigit()]
        self.set_no, self.cores_count, self.utilisation = integers


class Scheduler:
    def __init__(self):
        self.schedule_matrix = []

    def ceat(self, tasks: List[Task]):
        """
        Output: A set of schedule tables
        """
        # To find frame size: calculate minimum remaining period among all tasks

        time = 0
        while time < TOTAL_TIME:
            # TODO: update remaning time after each iteration
            current_frame: float = min([task.period for task in tasks])

            clusters: Set[Cluster] = self.construct_clusters(tasks, current_frame)
            schedule_matrix = self.construct_schedule(clusters, current_frame)
            schedule_matrix = self.ea_allocate()

            for task in tasks:
                task.invalidate_data()

            time += current_frame

    def construct_clusters(self, tasks: List[Task], frame_size: float) -> Set[Cluster]:
        """
        Output: Set of clusters
        """

        def find_lowest_in_share_matrix() -> Tuple[Optional[Task], Optional[Core]]:
            """
            Find the task which has lowest share value and does not belong to any
            existing cluster
            """
            lowest_value: Optional[float] = None
            # task associated with lowest share value
            task_lowest: Optional[Task] = None
            # core associated with lowest share value
            core_lowest: Optional[Core] = None

            for task in tasks:
                if task not in task_to_cluster_allocation.keys():
                    shares = task.get_shares(frame_size)  # task shares
                    if lowest_value is None or min(shares) < lowest_value:
                        lowest_value = min(shares)
                        task_lowest = task
                        core_lowest = Core.core_mapping[shares.index(min(shares))]

            return (task_lowest, core_lowest)

        def find_second_lowest_utilization_for_task(task: Task) -> Core:
            """
            Find a core such that it does not belong to a cluster and share value is the
            next lowest value
            """
            lowest_value = min(task.get_shares(frame_size))
            min_value: Optional[float] = None
            # core associated with second lowest share value
            min_core: Optional[Core] = None

            for core_index, share in enumerate(task.get_utilizations()):
                if core_index not in core_to_cluster_allocation.keys():
                    if min_value is None or (lowest_value < share < min_value):
                        min_core = Core.core_mapping[core_index]
                        min_value = share

            assert min_core is not None

            return min_core

        task_to_cluster_allocation: Mapping[Task, Cluster] = dict()
        core_to_cluster_allocation: Mapping[Core, Cluster] = dict()

        clusters: Set[Cluster] = set()

        while len(task_to_cluster_allocation) < len(tasks):
            task, core1 = find_lowest_in_share_matrix()

            if task is None or core1 is None:
                print("Infeasible task set")
                exit(1)

            if core1 not in core_to_cluster_allocation.keys():
                core2 = find_second_lowest_utilization_for_task(task)

                # TODO: line 9, check average computation demand
                # TODO: how to check computation demand
                new_cluster = Cluster(core1, core2, frame_size)
                clusters.add(new_cluster)

                new_cluster.tasks.add(task)

                core_to_cluster_allocation[core1] = new_cluster
                core_to_cluster_allocation[core2] = new_cluster
                task_to_cluster_allocation[task] = new_cluster
                # TODO: line 12, update spare capacity of the cluster `new_cluster`
            else:
                cluster = core_to_cluster_allocation[
                    core1
                ]  # cluster to which core1 belongs

                # TODO: line 16 and 17
                # TODO: update spare capacity and update task remaining period
                cluster.tasks.add(task)
                task_to_cluster_allocation[task] = cluster

        return clusters

    def construct_schedule(self, clusters: Set[Cluster], frame_size: int):
        """
        Output: Schedule matrix
        """
        # TODO: line 1, how is remaining capacity calculated and initialized?
        rem_cap: List[float] = [frame_size] * NO_OF_CORES  # remaining capacity of cores

        for cluster in clusters:
            core1 = cluster.core1
            core2 = cluster.core2

            tasks: Set[Task] = cluster.tasks
            tasks_sorted: List[Task] = sorted(
                tasks,
                key=lambda task: task.get_utilizations()[core1.id]
                / task.get_utilizations()[core2.id],
            )

            while (
                tasks_sorted
                and rem_cap[core1.id] > 0
                and tasks_sorted[0].get_utilizations()[core1.id]
                <= tasks_sorted[0].get_utilizations()[core2.id]
            ):
                task = tasks_sorted.pop(0)
                # TODO: line 9, assign the task to core 1
                rem_cap[core1.id] -= task.get_shares(frame_size)[core1.id]

            while tasks_sorted and rem_cap[core2.id] > 0:
                task = tasks_sorted.pop(-1)
                # TODO: line 13, assign the task to core 2
                rem_cap[core2.id] -= task.get_shares(frame_size)[core2.id]

            if tasks_sorted:
                pass
                # TODO: line 16, assign remaining tasks to core 1

            # TODO: line 17, update Schedule Matrix with initial schedule for cores in
            # cluster

    def ea_allocate(self, clusters: Set[Cluster]):
        """
        Output: Schedule Matrix SM_k for the current frame
        """
        # TODO: we do not have set of frequencies
        for cluster in clusters:
            for core in cluster.cores():
                pass
