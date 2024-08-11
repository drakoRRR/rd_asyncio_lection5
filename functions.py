import os
import multiprocessing as mp

from gc import disable as gc_disable, enable as gc_enable


def get_file_chunks(file_name: str, max_cpu: int = 8) -> tuple[int, list[tuple[int, int]]]:
    """Split file into chunks based on byte positions."""
    cpu_count = min(max_cpu, mp.cpu_count())

    file_size = os.path.getsize(file_name)
    chunk_size = file_size // cpu_count

    start_end = []
    with open(file_name, mode="rb") as f:
        def is_new_line(position):
            if position == 0:
                return True
            else:
                f.seek(position - 1)
                return f.read(1) == b"\n"

        def next_line(position):
            f.seek(position)
            f.readline()
            return f.tell()

        chunk_start = 0
        while chunk_start < file_size:
            chunk_end = min(file_size, chunk_start + chunk_size)

            while not is_new_line(chunk_end):
                chunk_end -= 1

            if chunk_start == chunk_end:
                chunk_end = next_line(chunk_end)

            start_end.append((chunk_start, chunk_end))
            chunk_start = chunk_end

    return cpu_count, start_end


def mp_count_words(file_name: str, chunk_start: int, chunk_end: int, counter, lock) -> dict:
    """Process a chunk of the file."""
    words = {}
    with open(file_name, mode="rb") as f:
        f.seek(chunk_start)
        gc_disable()
        for line in f:
            chunk_start += len(line)
            if chunk_start > chunk_end:
                break
            _word, _, match_count, _ = line.split(b"\t")
            _word = _word.decode("utf-8")
            if _word in words:
                words[_word] += int(match_count)
            else:
                words[_word] = int(match_count)

        gc_enable()

    with lock:
        counter.value += 1
    return words
