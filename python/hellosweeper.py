from benchmark.sweeper.sweep import Sweeper
from benchmark.data.config import ApplicationParameter, ApplicationParameters, ApplicationParameterConstraint, \
    ParameterBinding
from benchmark.data.metric import LongMetric
from random import randrange


def bench(config):
    value = int(config['prime']) + int(config['odd']) + int(config['even']) + int(config['fibo']) + randrange(5)
    print("test of", config, ":", value)
    return LongMetric(value)


test = 10

prime = ApplicationParameter("prime", 1, ["1", "2", "3", "5", "7", "11"])
odd = ApplicationParameter("odd", 2, ["1", "3", "5", "7", "9", "11"])
even = ApplicationParameter("even", 3, ["2", "4", "6", "8", "10", "12"])
fibo = ApplicationParameter("fibo", 4, ["1", "2", "3", "5", "8", "13", "21"])

# constraints = None
constraints = [
    # if prime==1, then odd=3, even=[4,10]
    ApplicationParameterConstraint(source=ParameterBinding(name="prime", value="1"),
                                   targets=[ParameterBinding(name="odd", value="3"),
                                            ParameterBinding(name="even", value="4"),
                                            ParameterBinding(name="even", value="10")]),
    # if fibo==13, then prime=1,odd=3,even=4
    ApplicationParameterConstraint(source=ParameterBinding(name="fibo", value="13"),
                                   targets=[ParameterBinding(name="prime", value="1"),
                                            ParameterBinding(name="odd", value="3"),
                                            ParameterBinding(name="even", value="4")]),
]
parameters = ApplicationParameters(parameters=[prime, odd, even, fibo], constraints=constraints)
sweeper = Sweeper(parameters, 5, remove_workdir=True)

print(sweeper)

config = sweeper.get_next()
while config != None:
    for _ in range(test):
        sweeper.score(config, bench(config))
    # print(f"current best {sweeper.best()}")
    sweeper.done(config)
    config = sweeper.get_next()
print("THE BEST IS", sweeper.best(), sweeper.get_score(sweeper.best()))
