from sweep import Sweeper
from data.config import ApplicationParameter
from data.metric import LongMetric


def bench(config):
    value = int(config['prime']) + int(config['odd']) + int(config['even']) + int(config['fibo'])
    print(config, value)
    return LongMetric(value)


prime = ApplicationParameter("prime", 1, ["1", "2", "3", "5", "7", "11"], None)
odd = ApplicationParameter("odd", 2, ["1", "3", "5", "7", "9", "11"], None)
even = ApplicationParameter("even", 3, ["2", "4", "6", "8", "10", "12"], None)
fibo = ApplicationParameter("fibo", 4, ["1", "1", "2", "3", "5", "8", "13", "21"], None)

sweeper = Sweeper([prime, odd, even, fibo], 4)

print(sweeper)
while sweeper.has_next:
    config = sweeper.get_next()
    sweeper.score(config, bench(config))
    print(f"current best {sweeper.best()}")
    # sweeper.done(config)
#     print("OK")
