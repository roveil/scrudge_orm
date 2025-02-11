from asyncio import gather
from typing import Any, Coroutine, Dict, List


async def gather_map(tasks_mapping: Dict[str, Coroutine | List[Coroutine]]) -> Dict[str, Any]:
    for key, task_or_tasks in tasks_mapping.items():
        if isinstance(task_or_tasks, list):
            tasks_mapping[key] = gather(*task_or_tasks)  # type: ignore

    results = await gather(*tasks_mapping.values())  # type: ignore

    # since python 3.6 we can do like this
    for index, key in enumerate(tasks_mapping.keys()):
        tasks_mapping[key] = results[index]

    return tasks_mapping
