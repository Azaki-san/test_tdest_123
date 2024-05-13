import multiprocessing
import os
import socket
import threading
import time

SERVER_URL = '127.0.0.1:12345'
CLIENT_BUFFER = 1024
UNSORTED_FILES_COUNT = 100
# On my m1 processor if threads will be equal to 100, execution will be faster
THREADS_PER_SESSION = 20


def create_directories():
    if not os.path.exists('unsorted_files'):
        os.mkdir('unsorted_files')

    if not os.path.exists('sorted_files'):
        os.mkdir('sorted_files')


def download_unsorted_files(i):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        ip, port = SERVER_URL.split(':')
        s.connect((ip, int(port)))
        file = b''
        while True:
            packet = s.recv(CLIENT_BUFFER)
            if not packet:
                break
            file += packet
        with open(f'unsorted_files/{i}.txt', 'wb') as f:
            f.write(file)
    lock.release()


def create_sorted_file(unsorted_id):
    with open(f"unsorted_files/{unsorted_id}.txt", "r") as unsorted_file:
        unsorted_list = [int(number)
                         for number in unsorted_file.read().split(',')]

        sorted_list = sorted(unsorted_list)

        with open(f"sorted_files/{unsorted_id}.txt", "w") as sorted_file:
            sorted_file.write(','.join(map(str, sorted_list)))


if __name__ == '__main__':
    lock = threading.Semaphore(THREADS_PER_SESSION)
    create_directories()
    tdownload0 = time.monotonic()
    threads = []
    for _ in range(UNSORTED_FILES_COUNT):
        lock.acquire()
        new_thread = threading.Thread(target=download_unsorted_files, args=(_,))
        threads.append(new_thread)
        new_thread.start()
    for thread in threads:
        thread.join()
    tdownload = time.monotonic() - tdownload0
    print(f"Files download time: {tdownload}")

    tsort0 = time.monotonic()
    sort_start = time.monotonic()
    with multiprocessing.Pool(processes=os.cpu_count()) as pool:
        pool.map(create_sorted_file, range(UNSORTED_FILES_COUNT))
    tsort = time.monotonic() - tsort0
    print(f"Sorting time: {tsort}")