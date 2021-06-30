import csv
import random
import os
from typing import List


def generate_file(
    global_utilization: int,
    cores_count: int,
    task_count: int,
    periods: List[int],
    utiliations_ratios: List[float],
):
    assert len(utilization_ratios) == cores_count

    set_no = 1
    while True:
        filename = f"set_{set_no}_cores_{cores_count}_utilization_{global_utilization}_tasks_{tasks_count}.csv"
        if os.path.exists(filename):
            set_no += 1
        else:
            break

    with open(filename, "w") as file:
        csv_writer = csv.writer(file)

        csv_writer.writerow(
            ["id", "exec", "period"] + [f"ratio{i}" for i in range(cores_count)]
        )

        for id in range(tasks_count):
            period: int = random.choice(periods)
            execution_requirement: float = (period / 100) * global_utilization

            csv_writer.writerow(
                [id, execution_requirement, period]
                + random.sample(utilization_ratios, cores_count)
            )

    print(f"File written to {filename}")


if __name__ == "__main__":
    cores_count: int = 4
    global_utilization: int = int(input("Enter utilization (default 40): ") or 40)
    tasks_count: int = int(input("Enter tasks count (default 20): ") or 20)
    periods: List[int] = [20, 50, 100, 200]
    utilization_ratios: List[float] = [1, 0.8, 1.2, 0.9]
    generate_file(
        global_utilization, cores_count, tasks_count, periods, utilization_ratios
    )
