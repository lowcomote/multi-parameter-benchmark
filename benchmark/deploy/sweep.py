from execo_engine import ParamSweeper, sweep
from pathlib import Path

# TODO : extend execo_engine.ParamSweeper
# https://github.com/lovasoa/execo/blob/master/src/execo_engine/sweep.py
# with tree + heurstic mechanism

parameters = dict(
    param1=["param1_1","param1_2"], 
    param2=["param2_1","param2_2"], 
    param3=["param3_1","param3_2"], 
    param4=["param4_1","param4_2"]
    )

sweeps = sweep(parameters)
sweeper = ParamSweeper(
    persistence_dir=str(Path("my_sweep")), sweeps=sweeps, save_sweeps=True
)

def bench(p):
    p1 = p["param1"]
    p2 = p["param2"]
    p3 = p["param3"]
    p4 = p["param4"]
    print(f"{p1}; {p2}; {p3}; {p4}")

parameter = sweeper.get_next()
while parameter:
    try:
        bench(parameter)
    except Exception as e:
        sweeper.skip(parameter)
    finally:
        parameter = sweeper.get_next()
