from benchmark.sweeper.sweep import Sweeper
from benchmark.data.config import ApplicationParameter
from benchmark.data.metric import LongMetric

def bench(config):
    value = int(config['prime']) + int(config['odd']) + int(config['even']) + int(config['fibo'])
    print("test of", config,":", value)
    return LongMetric(value)


prime = ApplicationParameter("prime", 1, ["1", "2", "3", "5", "7", "11"], None)
odd = ApplicationParameter("odd", 2, ["1", "3", "5", "7", "9", "11"], None)
even = ApplicationParameter("even", 3, ["2", "4", "6", "8", "10", "12"], None)
fibo = ApplicationParameter("fibo", 4, ["1", "2", "3", "5", "8", "13", "21"], None)

sweeper = Sweeper([prime, odd, even, fibo], 4, remove_workdir=True)

print(sweeper)

config = sweeper.get_next()
while config != None:
    sweeper.score(config, bench(config))
    # print(f"current best {sweeper.best()}")
    sweeper.done(config)
    config = sweeper.get_next()
print("THE BEST IS",sweeper.best(),sweeper.get_score(sweeper.best()))