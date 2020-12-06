import numpy as np
import multiprocessing as mp
import pandas as pd
from functools import partial
import numexpr

'''
    This func is the logic to be operated on each slice of data
'''
def DoMonth(months_, data):
    output = []
    for m in months_:
        df = data[data.month == m]
        # some heavy operation on the m'th slice, for instance,
        val = [m, df.expense.sum()]
        output.append(val)
    return output


class Parallelizer():
    def __init__(self):
        self.n_proc = mp.cpu_count()
        numexpr.set_num_threads(self.n_proc)

    '''
        This func is to parse the pieces of data returned by each processor
    '''
    def FlattenResults(self, res):
        flattenRes = []  # res is usually an [[[...]]]
        for p in res:
            for r in p:
                flattenRes.append(r)
        return pd.DataFrame(flattenRes, columns=["month", "expenses"])

    '''
        This func slices the data and assigns each piece to a processor
    '''
    def DoParallelLoop(self, months, DF) -> list:
        self.n_proc = min(self.n_proc, len(months))
        pool = mp.Pool(self.n_proc)
        print("  Parallel execution on {0} processors".format(self.n_proc))

        slices = np.array_split(months, self.n_proc)
        partial_func = partial(DoMonth, data=DF)
        res = pool.map(partial_func, iterable=slices)

        pool.close()
        pool.join()
        return self.FlattenResults(res)


'''
    USAGE EXAMPLE
'''
DF = pd.DataFrame({'month': range(1000), 'expense': np.random.normal(0, 1, 1000)})
pLoop = Parallelizer()
res = pLoop.DoParallelLoop(DF.month, DF)
res.head(2)
