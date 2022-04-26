"""
A module for quickly configuring a Spark standalone cluster on Grid5000

class: SparkG5kConf: the Spark configuration for G5k
"""
import subprocess

from enoslib import *
from enoslib.infra.enos_g5k.g5k_api_utils import get_api_username
from abc import ABC, abstractmethod

from typing import Dict

import os

__all__ = ['SparkG5kConf', 'SparkSubmit']


class SparkG5kConf:
    """
    SparkG5kConf to run Spark application on G5k

    Methods:
        start, stop
    
    Examples:

        .. code-block:: python

            from sparklib import SparkG5kConf
            
            ...

            g5kconf = SparkG5kConf(site="nancy", cluster="gros", worker=1, jobname="Spark_test", time="00:10:00", start="now")
            try:
                g5kconf.start()
                master = g5kconf.master
                username = g5kconf.username
                roles = g5kconf.roles
            except Exception as e:
                print(e)
            finally:
                g5kconf.stop()
    """

    _DEFAULT_TIME = "02:00:00"
    _DEFAULT_START = "now"
    _DEFAULT_JOB_NAME = "Spark_with_" + get_api_username()

    ROLE_MASTER = "master"
    ROLE_WORKER = "worker"

    # G5k cluster variables
    __roles = None
    __networks = None
    __provider = None
    __master = None

    def __init__(self, site: str, cluster: str, worker: int, jobname: str = _DEFAULT_JOB_NAME,
                 time: str = _DEFAULT_TIME, start: str = _DEFAULT_START):
        self.__site = site
        self.__cluster = cluster
        self.__worker = worker
        self.__time = time
        self.__start_time = start
        self.__username = get_api_username()
        self.__jobname = jobname

    def start(self):
        """
        Make a G5k on reservation, according to the fields of the current configuration.
        """
        my_network = G5kNetworkConf(id="n1", type="prod", roles=["my_network"], site=self.__site)

        # Setup g5k conf
        conf = None
        if self.__start_time == SparkG5kConf._DEFAULT_START:
            conf = (
                G5kConf.from_settings(job_type="allow_classic_ssh", job_name=self.__jobname, walltime=self.__time)
                    .add_network_conf(my_network)
                    .add_machine(
                    roles=[SparkG5kConf.ROLE_MASTER], cluster=self.__cluster, nodes=1, primary_network=my_network
                )
                    .add_machine(
                    roles=[SparkG5kConf.ROLE_WORKER], cluster=self.__cluster, nodes=self.__worker,
                    primary_network=my_network
                )
                    .finalize()
            )
        else:
            conf = (
                G5kConf.from_settings(job_type="allow_classic_ssh", job_name="Test_java_version", walltime=self.__time,
                                      reservation=self.__start_time)
                    .add_network_conf(my_network)
                    .add_machine(
                    roles=[SparkG5kConf.ROLE_MASTER], cluster=self.__cluster, nodes=1, primary_network=my_network
                )
                    .add_machine(
                    roles=[SparkG5kConf.ROLE_WORKER], cluster=self.__cluster, nodes=1, primary_network=my_network
                )
                    .finalize()
            )
        # Setup provider and reserve machines
        self.__provider = G5k(conf)
        self.__roles, self.__networks = self.__provider.init()

        # Start Spark Cluster
        self.__master = self.__roles[SparkG5kConf.ROLE_MASTER][0].address  # get master address

    def stop(self):
        """
        Cancel the G5K reservation.
        """
        self.__provider.destroy()

    @property
    def roles(self):
        return self.__roles

    @property
    def master(self):
        return self.__master

    @property
    def username(self):
        return self.__username


class SparkSubmit(ABC):
    _NO_PATHLOG = "-1"
    _NO_PATHERR = "-1"

    # Java and Spark versions related fields
    __isSetJava: bool = False
    __isSetSpark: bool = False
    __java = None  # Path to JAVA_HOME
    __java_home_backup = None  # Original value of the JAVA_HOME environmental variable
    __spark = None  # Path to SPARK_HOME

    def set_spark_path(self, path: str):
        """
        Set SPARK_HOME. Mandatory to submit Spark application.
        """
        self.__spark = path
        if self.__spark[-1] != "/":
            self.__spark = self.__spark + "/"
        self.__isSetSpark = True

    def set_java_path(self, path: str):
        """
        Set JAVA_HOME. By default, submission mechanism will use the installed java version.
        """
        self.__java = path
        if self.__java[-1] != "/":
            self.__java = self.__java + "/"
        self.__isSetJava = True

    def set_java_home(self):
        if not self.__isSetJava:
            return
        self.__java_home_backup = os.getenv("JAVA_HOME")
        os.environ["JAVA_HOME"] = self.__java

    def restore_java_home(self):
        if not self.__isSetJava:
            return
        if self.__java_home_backup is not None:
            # restore JAVA_HOME to its original value
            os.environ["JAVA_HOME"] = self.__java_home_backup
            self.__java_home_backup = None
        else:
            # delete JAVA_HOME as it was not set originally
            os.environ.pop("JAVA_HOME")

    def start(self):
        """
        Deploy the Spark cluster so that the application can be submitted.
        """
        if not self.__isSetSpark:
            raise Exception("The path to SPARK_HOME must be defined. Please, use set_spark_path(path).")

        self._on_start()

    def stop(self):
        """
        Stop current Spark cluster.
        """
        self._on_stop()

    def test_with_log(self, path_log: str = "/tmp/out.log", path_err: str = "/tmp/out.err"):
        """ Run a simple test on the cluster using files as output.

        Args:
            path_log:
                Path to the file the standard output will be printed in.
            path_err:
                Path to the file the error output will be printed in.
        """
        cmd = "ls {0}examples/jars/*examples*jar".format(self.__spark)
        with os.popen(cmd) as proc:
            jar_path = proc.read()
        self.submit_with_log(jar_path.rstrip("\n"), "org.apache.spark.examples.SparkPi", java_args={"": "10"},
                             path_log=path_log, path_err=path_err)

    def test(self):
        """ Run a simple test on the cluster. """
        self.test_with_log(SparkSubmit._NO_PATHLOG, SparkSubmit._NO_PATHERR)

    def submit_with_log(self, path_jar: str, classname: str, spark_args: Dict = {}, java_args: Dict = {},
                        path_log: str = "/tmp/out.log", path_err: str = "/tmp/out.err"):
        """
        Submit a Spark job on the cluster using files as output.

        Args:
            path_jar:
                Path to the program as a jar file.
            classname:
                Main class to execute in path_jar.
            spark_args:
                A dictionary of Spark argument.
            java_args:
                A dictionary of Java argument for the main program. If arguments `arg` has no name, please use {"":arg}.
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

        self._on_submit(path_jar, classname, str_spark_args, str_java_args, path_log, path_err)

    def submit(self, path_jar: str, classname: str, spark_args: Dict = {}, java_args: Dict = {}):
        """ Submit a Spark job on the cluster.

        Args:
            path_jar:
                Path to the program as a jar file.
            classname:
                Main class to execute in path_jar.
            spark_args:
                A dictionary of Spark argument.
            java_args:
                A dictionary of Java argument for the main program. If arguments `arg` has no name, please use {"":arg}.
        """
        self.submit_with_log(path_jar, classname, spark_args, java_args, SparkSubmit._NO_PATHLOG,
                             SparkSubmit._NO_PATHERR)

    @abstractmethod
    def _on_submit(self, path_jar: str, classname: str, spark_args: str, java_args: str, path_log: str = "/tmp/out.log",
                   path_err: str = "/tmp/out.err"):
        pass

    @abstractmethod
    def _on_start(self):
        pass

    @abstractmethod
    def _on_stop(self):
        pass


class LocalSparkSubmit(SparkSubmit):
    """
    SparkSubmit to deploy Spark applications on Spark clusters running on localhost.

    Methods:
        setJavaPath, setSparkPath,
        start, stop,
        testWithLog, test,
        submitWithLog, submit

    Examples:
        .. code-block:: python
            from sparklib import SparkSubmit

            ...
            # Need to setup JAVA_HOME and SPARK_HOME variables
            number_of_cores = 1
            spark_submit = LocalSparkSubmit(number_of_cores)
            spark_submit.set_java_path(JAVA_HOME)
            spark_submit.set_spark_path(SPARK_HOME)
            try:
                spark_submit.start()
                spark_submit.test_with_log()
                jar_path = ... # Path to 'example.jar' file, usually contained in SPARK_HOME/examples/jars/
                spark_submit.submit_with_log(jar_path, "org.apache.spark.examples.SparkPi", java_args={"": "10"})
            except Exception as e:
                print(e)
            finally:
                spark_submit.stop()
    """

    def __init__(self, number_of_cores=1):
        self.__number_of_cores = number_of_cores

    def _on_start(self):
        """
        Start current spark cluster.
        """
        if not self.__isSetSpark:
            raise Exception("The path to SPARK_HOME must be defined. Please, use sparkSubmit.set_spark_path(path).")

        # Start Spark Cluster
        try:
            self.set_java_home()
            cmd = "{0}sbin/start-master.sh -p 7077".format(self.__spark)
            subprocess.run(cmd, shell=True, capture_output=True, check=True)
            cmd = "{0}sbin/start-worker.sh spark://localhost:7077".format(self.__spark)
            subprocess.run(cmd, shell=True, capture_output=True, check=True)
        finally:
            self.restore_java_home()

    def _on_stop(self):
        """
        Stop current Spark cluster.
        """
        try:
            self.set_java_home()
            cmd = "{0}sbin/stop-all.sh".format(self.__spark)
            subprocess.run(cmd, shell=True, capture_output=True, check=True)
        finally:
            self.restore_java_home()

    def _on_submit(self, path_jar: str, classname: str, spark_args: str, java_args: str, path_log: str = "/tmp/out.log",
                   path_err: str = "/tmp/out.err"):
        """
       Submit a Spark job on the cluster using files as output.

       Args:
           path_jar:
               Path to the program as a jar file.
           classname:
               Main class to execute in path_jar.
           spark_args:
               A string of Spark arguments.
           java_args:
               A string of Java arguments for the main program.
           path_log:
               Path to the file the standard output will be printed in.
           path_err:
               Path to the file the error output will be printed in.
       """
        try:
            self.set_java_home()
            shell_out_log = (">> {0}".format(path_log)) if path_log != SparkSubmit._NO_PATHLOG else ""
            shell_out_err = ("2>> {0}".format(path_err)) if path_log != SparkSubmit._NO_PATHERR else ""
            cmd = "{0}bin/spark-submit --master spark://local[{1}] {2} --class {3} {4} {5} {6} {7}".format(
                self.__spark, self.__number_of_cores, spark_args, classname, path_jar, java_args,
                shell_out_log, shell_out_err)
            subprocess.run(cmd, shell=True, capture_output=True, check=True)
        except Exception as e:
            print(e)
        finally:
            cmd = "mv {0} {1}".format(path_log, "~")
            subprocess.run(cmd)
            cmd = "mv {0} {1}".format(path_err, "~")
            subprocess.run(cmd)
            self.restore_java_home()


class G5kSparkSubmit(SparkSubmit):
    """
    SparkSubmit to deploy Spark applications on Spark clusters in G5k.

    Methods:
        setJavaPath, setSparkPath,
        start, stop,
        testWithLog, test,
        submitWithLog, submit

    Examples:
        .. code-block:: python
            from sparklib import SparkSubmit

            ...
            # Need to setup JAVA_HOME and SPARK_HOME variables
            spark_submit = G5kSparkSubmit(username, roles)
            spark_submit.set_java_path(JAVA_HOME)
            spark_submit.set_spark_path(SPARK_HOME)
            try:
                spark_submit.start()
                spark_submit.test_with_log()
                jar_path = ... # Path to 'example.jar' file, usually contained in SPARK_HOME/examples/jars/
                spark_submit.submit_with_log(jar_path, "org.apache.spark.examples.SparkPi", java_args={"": "10"})
            except Exception as e:
                print(e)
            finally:
                spark_submit.stop()
    """

    __master = None  # Address of master on the current cluster

    def __init__(self, username, roles):
        self.__username = username
        self.__roles = roles

    def _on_start(self):
        """
        Deploy the Spark cluster.
        """
        if not self.__isSetSpark:
            raise Exception("The path to SPARK_HOME must be defined. Please, use sparkSubmit.set_spark_path(path).")

        # Start Spark Cluster
        self.__master = self.__roles[SparkG5kConf.ROLE_MASTER][0].address  # get master address
        try:
            # TODO this will not work correctly, because set_java_home() must be run on the remote machine!!!
            self.set_java_home()
            with play_on(pattern_hosts=SparkG5kConf.ROLE_MASTER, roles=self.__roles, run_as=self.__username) as p:
                cmd = "{0}sbin/start-master.sh -p 7077".format(self.__spark)
                p.shell(cmd)
            with play_on(pattern_hosts=SparkG5kConf.ROLE_WORKER, roles=self.__roles, run_as=self.__username) as p:
                cmd = "{0}sbin/start-worker.sh spark://{1}:7077".format(self.__spark, self.__master)
                p.shell(cmd)
        finally:
            # TODO this will not work correctly, because restore_java_home() must be run on the remote machine!!!
            self.restore_java_home()

    def _on_stop(self):
        """
        Stop current Spark cluster.
        """
        try:
            # TODO this will not work correctly, because set_java_home() must be run on the remote machine!!!
            self.set_java_home()
            cmd = "{0}sbin/stop-all.sh".format(self.__spark)
            with play_on(pattern_hosts=SparkG5kConf.ROLE_MASTER, roles=self.__roles, run_as=self.__username) as p:
                p.shell(cmd)
            with play_on(pattern_hosts=SparkG5kConf.ROLE_WORKER, roles=self.__roles, run_as=self.__username) as p:
                p.shell(cmd)
        finally:
            # TODO this will not work correctly, because restore_java_home() must be run on the remote machine!!!
            self.restore_java_home()

    def _on_submit(self, path_jar: str, classname: str, spark_args: str, java_args: str,
                   path_log: str = "/tmp/out.log", path_err: str = "/tmp/out.err"):
        """
        Submit a Spark job on the cluster using files as output.

        Args:
            path_jar:
                Path to the program as a jar file.
            classname:
                Main class to execute in path_jar.
            spark_args:
                A string of Spark arguments.
            java_args:
                A string of Java arguments for the main program.
            path_log:
                Path to the file the standard output will be printed in.
            path_err:
                Path to the file the error output will be printed in.
        """

        # Run spark-submit
        try:
            # TODO this will not work correctly, because set_java_home() must be run on the remote machine!!!
            self.set_java_home()
            shell_out_log = (">> {0}".format(path_log)) if path_log != SparkSubmit._NO_PATHLOG else ""
            shell_out_err = ("2>> {0}".format(path_err)) if path_log != SparkSubmit._NO_PATHERR else ""
            with play_on(pattern_hosts=SparkG5kConf.ROLE_MASTER, roles=self.__roles, run_as=self.__username) as p:
                try:
                    cmd = "{0}bin/spark-submit --master spark://{1}:7077 {2} --class {3} {4} {5} {6} {7}".format(
                        self.__spark, self.__master, spark_args, classname, path_jar, java_args,
                        shell_out_log, shell_out_err)
                    p.shell(cmd)
                    p.fetch(src=path_log, dest="~")
                    p.fetch(src=path_err, dest="~")
                    os.system("rm -rf {0}work".format(self.__spark))
                except Exception as e:
                    print(e)
                    p.fetch(src=path_log, dest="~")
                    p.fetch(src=path_err, dest="~")
        finally:
            # TODO this will not work correctly, because restore_java_home() must be run on the remote machine!!!
            self.restore_java_home()
