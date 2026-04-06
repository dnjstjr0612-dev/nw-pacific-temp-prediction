import sys
import time
from pathlib import Path
import requests
from netCDF4 import Dataset

SAVE_DIR = Path("/data3/CESM_HR/SST")
SAVE_DIR.mkdir(parents=True, exist_ok=True)

BASE = "https://osdf-director.osg-htc.org/ncar/gdex/d651029/B.E.13.B1850C5.ne120_t12.sehires38.003.sunway_02/ocn/proc/tseries/month_1"

FILES = [
    "B.E.13.B1850C5.ne120_t12.sehires38.003.sunway_02.pop.h.SST.025001-025912.nc",
    "B.E.13.B1850C5.ne120_t12.sehires38.003.sunway_02.pop.h.SST.026001-026912.nc",
    "B.E.13.B1850C5.ne120_t12.sehires38.003.sunway_02.pop.h.SST.027001-027912.nc",
    "B.E.13.B1850C5.ne120_t12.sehires38.003.sunway_02.pop.h.SST.028001-028912.nc",
    "B.E.13.B1850C5.ne120_t12.sehires38.003.sunway_02.pop.h.SST.029001-029912.nc",
    "B.E.13.B1850C5.ne120_t12.sehires38.003.sunway_02.pop.h.SST.030001-030912.nc",
    "B.E.13.B1850C5.ne120_t12.sehires38.003.sunway_02.pop.h.SST.031001-031912.nc",
    "B.E.13.B1850C5.ne120_t12.sehires38.003.sunway_02.pop.h.SST.032001-032912.nc",
    "B.E.13.B1850C5.ne120_t12.sehires38.003.sunway_02.pop.h.SST.033001-033712.nc",
    "B.E.13.B1850C5.ne120_t12.sehires38.003.sunway_02.pop.h.SST.033801-033912.nc",
    "B.E.13.B1850C5.ne120_t12.sehires38.003.sunway_02.pop.h.SST.034001-034912.nc",
    "B.E.13.B1850C5.ne120_t12.sehires38.003.sunway_02.pop.h.SST.035001-035912.nc",
    "B.E.13.B1850C5.ne120_t12.sehires38.003.sunway_02.pop.h.SST.036001-036912.nc",
    "B.E.13.B1850C5.ne120_t12.sehires38.003.sunway_02.pop.h.SST.037001-037912.nc",
    "B.E.13.B1850C5.ne120_t12.sehires38.003.sunway_02.pop.h.SST.038001-038912.nc",
    "B.E.13.B1850C5.ne120_t12.sehires38.003.sunway_02.pop.h.SST.039001-039912.nc",
    "B.E.13.B1850C5.ne120_t12.sehires38.003.sunway_02.pop.h.SST.040001-040912.nc",
    "B.E.13.B1850C5.ne120_t12.sehires38.003.sunway_02.pop.h.SST.041001-041912.nc",
    "B.E.13.B1850C5.ne120_t12.sehires38.003.sunway_02.pop.h.SST.042001-042912.nc",
    "B.E.13.B1850C5.ne120_t12.sehires38.003.sunway_02.pop.h.SST.043001-043912.nc",
    "B.E.13.B1850C5.ne120_t12.sehires38.003.sunway_02.pop.h.SST.044001-044912.nc",
    "B.E.13.B1850C5.ne120_t12.sehires38.003.sunway_02.pop.h.SST.045001-045912.nc",
    "B.E.13.B1850C5.ne120_t12.sehires38.003.sunway_02.pop.h.SST.046001-046912.nc",
    "B.E.13.B1850C5.ne120_t12.sehires38.003.sunway_02.pop.h.SST.047001-047912.nc",
    "B.E.13.B1850C5.ne120_t12.sehires38.003.sunway_02.pop.h.SST.048001-048912.nc",
    "B.E.13.B1850C5.ne120_t12.sehires38.003.sunway_02.pop.h.SST.049001-049912.nc",
    "B.E.13.B1850C5.ne120_t12.sehires38.003.sunway_02.pop.h.SST.050001-050912.nc",
    "B.E.13.B1850C5.ne120_t12.sehires38.003.sunway_02.pop.h.SST.050201-051912.nc",
    "B.E.13.B1850C5.ne120_t12.sehires38.003.sunway_02.pop.h.SST.051001-051912.nc",
]

CHUNK = 4 * 1024 * 1024
RETRIES = 10
WAIT = 20

def validate_nc(path: Path) -> bool:
    try:
        ds = Dataset(str(path))
        ds.close()
        return True
    except Exception as e:
        print(f"  validation FAIL: {e}")
        return False

def download_file(fname: str) -> bool:
    url = f"{BASE}/{fname}"
    dest = SAVE_DIR / fname
    tmp = SAVE_DIR / f"{fname}.tmp"

    for attempt in range(1, RETRIES + 1):
        try:
            if tmp.exists():
                tmp.unlink()

            with requests.get(url, stream=True, timeout=120) as r:
                r.raise_for_status()
                total = int(r.headers.get("Content-Length", 0))
                received = 0

                with open(tmp, "wb") as f:
                    for chunk in r.iter_content(chunk_size=CHUNK):
                        if chunk:
                            f.write(chunk)
                            received += len(chunk)
                            if total:
                                pct = 100 * received / total
                                print(f"\r  {pct:5.1f}%  {received/1e9:.2f}/{total/1e9:.2f} GB", end="", flush=True)

            print()

            if total and tmp.stat().st_size != total:
                raise ValueError(f"size mismatch: {tmp.stat().st_size} != {total}")

            tmp.rename(dest)

            if not validate_nc(dest):
                raise ValueError("netCDF validation failed")

            print(f"  OK: {fname}")
            return True

        except Exception as e:
            print(f"  attempt {attempt}/{RETRIES} failed: {e}")
            if tmp.exists():
                tmp.unlink()
            if dest.exists():
                if validate_nc(dest):
                    return True
                else:
                    dest.unlink()
                dest.unlink()
            if attempt < RETRIES:
                print(f"  waiting {WAIT}s before retry...")
                time.sleep(WAIT)

    return False

failed = []
for i, fname in enumerate(FILES, 1):
    print(f"\n[{i}/{len(FILES)}] {fname}")
    ok = download_file(fname)
    if not ok:
        failed.append(fname)

print("\n" + "=" * 60)
if failed:
    print(f"FAILED {len(failed)} files:")
    for f in failed:
        print(" ", f)
    sys.exit(1)
else:
    print("ALL FILES DOWNLOADED AND VALIDATED")