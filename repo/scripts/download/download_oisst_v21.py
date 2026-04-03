from pathlib import Path
from datetime import date, timedelta
import subprocess
import ssl

#save_dir = Path.home() / "Documents/GitHub/Oceanography/nw-pacific-temp-prediction/repo/data/raw/oisst_v21/daily"
save_dir = Path("/data3/OISST_V21")
save_dir.mkdir(parents=True, exist_ok=True)

# ssl certificate -> unverified 
ssl._create_default_https_context = ssl._create_unverified_context

start = date(1993, 1, 1)
end   = date(2024, 12, 31)

base = "https://www.ncei.noaa.gov/data/sea-surface-temperature-optimum-interpolation/v2.1/access/avhrr"

d = start
while d <= end:
    ymd = d.strftime("%Y%m%d")
    ym = d.strftime("%Y%m")
    fname = f"oisst-avhrr-v02r01.{ymd}.nc"
    url = f"{base}/{ym}/{fname}"
    out = save_dir / fname

    if out.exists():
        print(f"skip  {fname}")
        d += timedelta(days=1)
        continue

    print(f"get   {fname}")
    result = subprocess.run(
        ["curl", "-L", "-o", str(out), url],
        capture_output=True,
        text=True
    )

    if result.returncode != 0:
        print(f"ERR  {fname} : {result.stderr.strip()}")
        if out.exists() and out.stat().st_size == 0:
            out.unlink()

    d += timedelta(days=1)