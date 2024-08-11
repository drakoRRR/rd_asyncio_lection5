import asyncio
import time
import multiprocessing as mp
from contextlib import contextmanager
from concurrent.futures import ProcessPoolExecutor
from functions import mp_count_words, get_file_chunks

FILE_PATH = "./googlebooks-eng-all-1gram-20120701-a"
WORD = "ära"


@contextmanager
def timer(msg: str):
    start = time.perf_counter()
    yield
    print(f"{msg} took {time.perf_counter() - start:.2f} seconds")


def reduce_words(target: dict, source: dict) -> dict:
    for key, value in source.items():
        if key in target:
            target[key] += value
        else:
            target[key] = value
    return target


async def monitoring(counter, counter_lock, total):
    interval_seconds = 1

    while True:
        print(f"Progress: {counter.value}/{total}")
        if counter.value == total:
            break
        await asyncio.sleep(interval_seconds)


async def main():
    loop = asyncio.get_event_loop()

    words = {}

    with timer("Reading file"):
        cpu_count, file_chunks = get_file_chunks(FILE_PATH)

    with mp.Manager() as manager:
        counter = manager.Value("i", 0)
        counter_lock = manager.Lock()

        monitoring_task = asyncio.shield(
            asyncio.create_task(monitoring(counter, counter_lock, len(file_chunks)))
        )

        with ProcessPoolExecutor() as executor:
            with timer("Processing data"):
                results = []
                for chunk_start, chunk_end in file_chunks:
                    results.append(
                        loop.run_in_executor(
                            executor,
                            mp_count_words,
                            FILE_PATH,
                            chunk_start,
                            chunk_end,
                            counter,
                            counter_lock,
                        )
                    )

                done, _ = await asyncio.wait(results)

        monitoring_task.cancel()

    with timer("Reducing results"):
        for result in done:
            words = reduce_words(words, result.result())

    with timer("Printing results"):
        print("Total words: ", len(words))
        print("Total count for word : ", words.get(WORD, 0))


if __name__ == "__main__":
    with timer("Total time"):
        asyncio.run(main())
