"""
src/spark_session.py
--------------------
Provides a reusable SparkSession factory.

Usage
-----
    from src.spark_session import get_spark_session
    spark = get_spark_session()
    sc = spark.sparkContext
"""

import os
import sys
import glob

# -- Auto-detect JAVA_HOME if not already set ----------------------------------
def _find_java_home() -> str | None:
    """Search common locations for a Java installation on Windows."""
    candidates = [
        # Antigravity IDE bundled JRE (most likely on this machine)
        os.path.expanduser(r"~\.antigravity\extensions\redhat.java-1.56.0-win32-x64\jre\21.0.12.1-win32-x86_64"),
        os.path.expanduser(r"~\.antigravity\extensions\redhat.java-1.55.0-win32-x64\jre\21.0.11-win32-x86_64"),
        # Standard install locations
        r"C:\Program Files\Java\jdk-21",
        r"C:\Program Files\Java\jdk-17",
        r"C:\Program Files\Java\jdk-11",
        r"C:\Program Files\Eclipse Adoptium\jdk-17.0.13.11-hotspot",
    ]
    # Also glob for any redhat.java extension
    antigravity_pattern = os.path.expanduser(r"~\.antigravity\extensions\redhat.java-*\jre\*")
    candidates.extend(sorted(glob.glob(antigravity_pattern), reverse=True))

    for path in candidates:
        if path and os.path.isfile(os.path.join(path, "bin", "java.exe")):
            return path
    return None

if not os.environ.get("JAVA_HOME"):
    java_home = _find_java_home()
    if java_home:
        os.environ["JAVA_HOME"] = java_home
        java_bin = os.path.join(java_home, "bin")
        if java_bin not in os.environ.get("PATH", ""):
            os.environ["PATH"] = java_bin + os.pathsep + os.environ.get("PATH", "")

# -- Make sure PySpark can find a Python interpreter ---------------------------
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
# Workaround for Python 3.12+ WinError 10038 on Windows
os.environ.setdefault("PYSPARK_PIN_THREAD", "true")

# -- Suppress noisy INFO logs from Spark / Hadoop -----------------------------
import logging
logging.getLogger("py4j").setLevel(logging.ERROR)

from pyspark.sql import SparkSession


def get_spark_session(
    app_name: str = "ECommerce_Sales_Analysis",
    master: str = "local[*]",
    driver_memory: str = "2g",
    log_level: str = "WARN",
) -> SparkSession:
    """
    Create (or retrieve existing) a SparkSession configured for
    local execution on a Windows machine.

    Parameters
    ----------
    app_name : str
        Name shown in the Spark UI.
    master : str
        Spark master URL.  ``local[*]`` uses all available CPU cores.
    driver_memory : str
        JVM driver heap size (e.g. ``"2g"``).
    log_level : str
        Spark log verbosity: ``"ERROR"`` | ``"WARN"`` | ``"INFO"``.

    Returns
    -------
    pyspark.sql.SparkSession
    """
    import pyspark
    pyspark_major = int(pyspark.__version__.split(".")[0])

    builder = (
        SparkSession.builder
        .appName(app_name)
        .master(master)
        .config("spark.driver.memory", driver_memory)
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.default.parallelism", "4")
        .config("spark.python.use.daemon", "false")
        .config("spark.ui.enabled", "true")
        .config("spark.ui.port", "4040")
    )

    # PySpark 3.x specific configs
    if pyspark_major < 4:
        builder = builder.config("spark.hadoop.fs.defaultFS", "file:///")

    builder = (
        builder
        .config("spark.python.worker.faulthandler.enabled", "true")
        .config("spark.sql.execution.pyspark.udf.faulthandler.enabled", "true")
    )

    # ── Windows + Python 3.12+ workaround ────────────────────────────────────
    # PySpark worker daemon crashes with WinError 10038 (WSAENOTSOCK) because
    # Python 3.12+ changed how socket handles are inherited by child processes.
    # We detect this case and return a pure-Python MockSparkSession that
    # executes all 13 RDD operations in-process without a JVM worker.
    if sys.platform == "win32" and sys.version_info >= (3, 12):
        import logging
        log = logging.getLogger(__name__)
        log.warning(
            "Windows + Python 3.12+ detected: PySpark RDD workers crash with "
            "WinError 10038. Returning MockSparkSession (pure-Python RDD emulation). "
            "For the live Spark Web UI, run with Python 3.11 or earlier."
        )
        from src.mock_spark import MockSparkSession as _Mock
        return _Mock()  # type: ignore[return-value]

    try:
        spark = builder.getOrCreate()
        spark.sparkContext.setLogLevel(log_level)
        return spark
    except Exception as e:
        import logging
        logging.getLogger(__name__).error(f"Failed to create SparkSession: {str(e)}")
        raise


def stop_spark_session(spark) -> None:
    """Gracefully stop the provided SparkSession (real or mock)."""
    if spark is not None:
        try:
            spark.stop()
        except Exception:
            pass
