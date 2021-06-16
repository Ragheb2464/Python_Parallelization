from joblib import Parallel, delayed
import datetime
import time

"""
    function which does the calculations for us (here we make the thread sleep for 30 seconds)
        @Note this must be a method or a static function 
        @Note make sure there is no resource that can cause deadlock
"""


def worker(i):
    time.sleep(10)

    return "something_{}".format(i)


"""
    Parallelism of some work
    
    Example: here we want to call the worker() N time 
    
     class joblib.Parallel(
                             n_jobs=None, 
                             backend=None, 
                             verbose=0, 
                             timeout=None, 
                             pre_dispatch='2 * n_jobs', 
                             batch_size='auto', 
                             temp_folder=None, 
                             max_nbytes='1M', 
                             mmap_mode='r', 
                             prefer=None, 
                             require=None
     )(
        delayed(__name_of_worker_func__)(__worker_args__)
    )
    
    Reference -> https://joblib.readthedocs.io/en/latest/parallel.html
    
    :res will be a list of results where each element is returned stuff of a processor
"""
N = 8

print("Starting parallel...")
start_time = time.time()
Parallel(n_jobs=-1)(delayed(worker)(i) for i in range(N))
print("Parallel took {}s".format(time.time() - start_time))

print("Starting sequential...")
start_time = time.time()
for i in range(N):
    worker(i)
print("Sequential took {}s".format(time.time() - start_time))
