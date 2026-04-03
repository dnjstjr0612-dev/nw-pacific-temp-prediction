# from src.paths import RAW_OISST, PROCESSED

from pathlib import Path
import socket

HOST = socket.gethostname().lower()

if HOST.startswith("acc") or "yonsei" in HOST:
    RAW_OISST = Path("/data3/OISST_V21")
    RAW_CESM = Path("/data3/CESM_HR/SST")
    PROCESSED = Path("/data3/leewonseok/processed")
    RESULTS = Path("/data3/leewonseok/results")
else:
    RAW_OISST = Path("data/sample/OISST_V21")
    RAW_CESM = Path("data/sample/CESM_HR")
    PROCESSED = Path("outputs")
    RESULTS = Path("outputs")