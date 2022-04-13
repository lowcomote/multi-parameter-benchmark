"""
A module for quickly configuring a Spark standalone cluster on Grid5000

class: SparkG5kConf: the Spark configuration for G5k
"""
from enoslib import *
from enoslib.infra.enos_g5k.g5k_api_utils import get_api_username

from typing import Any, Dict

import os

__all__ = ['SparkG5kConf']

_DEFAULT_TIME = "02:00:00"
_DEFAULT_START = "now"
_DEFAULT_JOB_NAME = "Spark_with_" + get_api_username()

_ROLE_MASTER = "master"
_ROLE_WORKER = "worker"

_NO_PATHLOG = "-1"
_NO_PATHERR = "-1"


class SparkG5kConf:
    """
    SparkG5kConf to run Spark application on G5k

    Methods:
        setJavaPath, setSparkPath,
        start, stop,
        testWithLog, test,
        submitWithLog, submit
    
    Examples:

        .. code-block:: python

            from sparklib import SparkG5kConf
            
            ... 
            # Need to setup JAVA_HOME and SPARK_HOME variables

            myspark = SparkG5kConf(site="nancy", cluster="gros", worker=1, jobname="Spark_test", time="00:10:00", start="now")
            myspark.setJavaPath(JAVA_HOME)
            myspark.setSparkPath(SPARK_HOME)
            try:
                myspark.start()
                myspark.testWithLog()
                jar_path = ... # Path to 'example.jar' file, usually contained in SPARK_HOME/examples/jars/
                myspark.submitWithLog(jar_path, "org.apache.spark.examples.SparkPi", java_args={"": "10"})
            except Exception as e:
                print(e)
            finally:
                myspark.stop()
    """

    # Java and Spark versions related fields
    __isSetJava: bool = False
    __isSetSpark: bool = False
    __java = None  # Path to JAVA_HOME
    __spark = None  # Path to SPARK_HOME
    __master = None  # Address of master on the current cluster

    # G5k cluster variables
    __roles = None
    __networks = None
    __provider = None

    def __init__(self, site: str, cluster: str, worker: int, jobname: str = _DEFAULT_JOB_NAME,
                 time: str = _DEFAULT_TIME, start: str = _DEFAULT_START):
        self.__site = site
        self.__cluster = cluster
        self.__worker = worker
        self.__time = time
        self.__start_time = start
        self.__username = get_api_username()
        self.__jobname = jobname

    def setJavaPath(self, path: str):
        """ set JAVA_HOME. By default, SparkG5kConf submission mechanism will use installed java version.
        """
        self.__java = path
        if self.__java[-1] != "/":
            self.__java = self.__java + "/"
        self.__isSetJava = True

    def setSparkPath(self, path: str):
        """ set SPARK_HOME. Mandatory to submit Spark application.
        """
        self.__spark = path
        if self.__spark[-1] != "/":
            self.__spark = self.__spark + "/"
        self.__isSetSpark = True

    def start(self):
        """ Make a G5k on reservation, according to the fields of the current configuration.
        """
        if self.__isSetSpark == False:
            raise Exception("The path to SPARK_HOME must be defined. Please, use sparkConf.setSparkPath(path).")

        my_network = G5kNetworkConf(id="n1", type="prod", roles=["my_network"], site=self.__site)

        # Setup g5k conf
        conf = None
        if self.__start_time == _DEFAULT_START:
            conf = (
                G5kConf.from_settings(job_type="allow_classic_ssh", job_name=self.__jobname, walltime=self.__time)
                    .add_network_conf(my_network)
                    .add_machine(
                    roles=[_ROLE_MASTER], cluster=self.__cluster, nodes=1, primary_network=my_network
                )
                    .add_machine(
                    roles=[_ROLE_WORKER], cluster=self.__cluster, nodes=self.__worker, primary_network=my_network
                )
                    .finalize()
            )
        else:
            conf = (
                G5kConf.from_settings(job_type="allow_classic_ssh", job_name="Test_java_version", walltime=self.__time,
                                      reservation=self.__start_time)
                    .add_network_conf(my_network)
                    .add_machine(
                    roles=[_ROLE_MASTER], cluster=self.__cluster, nodes=1, primary_network=my_network
                )
                    .add_machine(
                    roles=[_ROLE_WORKER], cluster=self.__cluster, nodes=1, primary_network=my_network
                )
                    .finalize()
            )
        # Setup provider and reserve machines
        self.__provider = G5k(conf)
        self.__roles, self.__networks = self.__provider.init()

        # Start Spark Cluster
        self.__master = self.__roles[_ROLE_MASTER][0].address  # get master address
        shellSetJava = ("JAVA_HOME=" + self.__java + " ") if self.__isSetJava else ""
        with play_on(pattern_hosts=_ROLE_MASTER, roles=self.__roles, run_as=self.__username) as p:
            p.shell(shellSetJava + self.__spark + "sbin/start-master.sh -p 7077")
        with play_on(pattern_hosts=_ROLE_WORKER, roles=self.__roles, run_as=self.__username) as p:
            p.shell(shellSetJava + self.__spark + "sbin/start-worker.sh spark://" + self.__master + ":7077")

    def stop(self):
        """ Stop current Spark cluster and cancel g5k reservation.
        """
        shellSetJava = ("JAVA_HOME=" + self.__java + " ") if self.__isSetJava else ""
        with play_on(pattern_hosts=_ROLE_MASTER, roles=self.__roles, run_as=self.__username) as p:
            p.shell(shellSetJava + self.__spark + "sbin/stop-all.sh")
        with play_on(pattern_hosts=_ROLE_WORKER, roles=self.__roles, run_as=self.__username) as p:
            p.shell(shellSetJava + self.__spark + "sbin/stop-all.sh")
        self.__provider.destroy()

    def testWithLog(self, path_log: str = "/tmp/out.log", path_err: str = "/tmp/out.err"):
        """ Run a simple test on the cluster using files as output.
        
        Args:
            path_log:
                Path to the file the standard output will be printed in.
            path_err:
                Path to the file the error output will be printed in.
        """
        stream = os.popen('ls ' + self.__spark + "examples/jars/*examples*jar")
        jar_path = stream.read()
        stream.close()
        self.submitWithLog(jar_path.rstrip("\n"), "org.apache.spark.examples.SparkPi", java_args={"": "10"})

    def test(self):
        """ Run a simple test on the cluster. """
        self.testWithLog(_NO_PATHLOG, _NO_PATHERR)

    def submitWithLog(self, path_jar: str, classname: str, spark_args: Dict = {}, java_args: Dict = {},
                      path_log: str = "/tmp/out.log", path_err: str = "/tmp/out.err"):
        """ Submit a Spark job on the cluster using files as output.
        
        Args:
            path_jar: 
                Path to the program as a jar file.
            classname:
                Main class to execute in path_jar.
            spark_args:
                A dictionnary of Spark argument.
            java_args:
                A dictionnary of Java argument for the main program. If arguments `arg` has no name, please use {"":arg}.
            path_log:
                Path to the file the standard output will be printed in.
            path_err:
                Path to the file the error output will be printed in.
        """
        # Get Spark arguments as a single string value
        str_spark_args = ""
        for arg in spark_args.keys():
            str_arg = arg
            if arg[0: 2] != "--":
                str_arg = "--" + arg
            value = spark_args.get(arg)
            str_spark_args = str_spark_args + str_arg + " " + value + " "
        # Get Java arguments (of the Jar) arguments as a single string value
        str_java_args = ""
        for arg in java_args.keys():
            value = java_args.get(arg)
            str_java_args = str_java_args + arg + " " + value + " "
        # Run spark-submit
        shellSetJava = ("JAVA_HOME=" + self.__java + " ") if self.__isSetJava else ""
        shellOutLog = (" >> " + path_log) if path_log != _NO_PATHLOG else ""
        shellOutErr = (" 2>> " + path_err) if path_log != _NO_PATHERR else ""
        with play_on(pattern_hosts=_ROLE_MASTER, roles=self.__roles, run_as=self.__username) as p:
            try:
                cmd = shellSetJava + self.__spark + "bin/spark-submit --master spark://" + self.__master + ":7077 " + str_spark_args + " --class " + classname + " " + path_jar + " " + str_java_args + shellOutLog + shellOutErr
                p.shell(cmd)
                p.fetch(src=path_log, dest="~")
                p.fetch(src=path_err, dest="~")
                os.system("rm -rf " + self.__spark + "work")
            except Exception as e:
                print(e)
                p.fetch(src=path_log, dest="~")
                p.fetch(src=path_err, dest="~")

    def submit(self, pathJar: str, classname: str, spark_args: Dict = {}, java_args: Dict = {}):
        """ Submit a Spark job on the cluster.
        
        Args:
            path_jar: 
                Path to the program as a jar file.
            classname:
                Main class to execute in path_jar.
            spark_args:
                A dictionnary of Spark argument.
            java_args:
                A dictionnary of Java argument for the main program. If arguments `arg` has no name, please use {"":arg}.
        """
        self.submitWithLog(pathJar, classname, spark_args, java_args, _NO_PATHLOG, _NO_PATHERR)
