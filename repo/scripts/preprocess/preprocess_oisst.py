# ==================================================
# 0. import and directory setting
# ==================================================
import xarray as xr
from pathlib import Path

# ── directory ─────────────────────────────────────
DATA_DIR   = Path(__file__).resolve().parents[2] / "data/raw/oisst_v21/daily"
OUTPUT_DIR = Path(__file__).resolve().parents[2] / "data/processed"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

Domains={
    "local_wnp": {"lat": (15.0, 45.0), "lon" : (120.0, 180.0)}, # local WNP box
    "trop_wnp": {"lat": (-10.0, 45.0), "lon" : (110.0, 180.0)}, # tropical-WNP connected box
    "north_pacific": {"lat": (20.0, 65.0), "lon" : (110.0, 220.0)}, # broader Pacific box
}

BASE_Domains = {"lat": (-10.0, 65.0), "lon": (110.0, 220.0)}
YEARS = range(1993, 2025)


# ==================================================
# 1. loop by year (BASE_Domains -> monthly concat)
# ==================================================
lat_min_a, lat_max_a = BASE_Domains["lat"]
lon_min_a, lon_max_a = BASE_Domains["lon"]

monthly_list = []
bad_files = []

for year in YEARS:
    files = sorted(DATA_DIR.glob(f"oisst-avhrr-v02r01.{year}*.nc"))
    if not files:
        print(f"{year} No exists")
        continue

    good_files = []
    for f in files:
        try:
            ds_tmp = xr.open_dataset(f)
            ds_tmp.close()
            good_files.append(f)
        except Exception:
            bad_files.append(f.name)

    if not good_files:
        print(f" {year} : no valid files")
        continue

    if len(good_files) < len(files):
        print(f"{year} error")

    ds = xr.open_mfdataset(good_files, combine="by_coords", parallel=False)
    sst = (ds["sst"].squeeze("zlev", drop=True).sel(\
        lat = slice(lat_min_a, lat_max_a), \
        lon = slice(lon_min_a, lon_max_a)))

    monthly_list.append(sst.resample(time="MS").mean().compute())
    ds.close()
    print(f"  {year}: complete")

if bad_files:
    print(f"\nerror files: {bad_files}")

if not monthly_list:
    raise RuntimeError(f"No data loaded. Check DATA_DIR: {DATA_DIR}")


# ==================================================
# 2. BASE_Domains -> each named domain subset
# ==================================================
sst_base = xr.concat(monthly_list, dim="time")
SST = {
    name: sst_base.sel(lat=slice(*dom["lat"]), lon=slice(*dom["lon"]))
    for name, dom in Domains.items()
}

for k, sst in SST.items():
    out_path = OUTPUT_DIR / f"oisst_monthly_nwpacific_{k}_1993_2024.nc"
    sst.to_dataset(name="sst").to_netcdf(
        out_path,
        encoding={"sst": {"zlib": True, "complevel": 4, "dtype": "float32"}},
    )
    print(f"저장: {out_path.name}  {dict(sst.sizes)}")
