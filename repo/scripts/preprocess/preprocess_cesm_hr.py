# ==================================================
# 0. import and directory setting
# ==================================================

import xarray as xr
import numpy as np
#LOCAL from pathlib import Path
from src.paths import RAW_CESM, PROCESSED # Server

# local
'''
DATA_DIR   = Path(__file__).resolve().parents[2] / "data/raw/cesm_hr/sst"
OUTPUT_DIR = Path(__file__).resolve().parents[2] / "data/processed"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
'''
# Server
DATA_DIR = RAW_CESM
OUTPUT_DIR = PROCESSED
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

'''
Example :
<xarray.Dataset> Size: 6GB
Dimensions:             (nlat: 2400, nlon: 3600, time: 120, z_t: 1, z_w: 62,
                         d2: 2, z_t_150m: 15, z_w_bot: 62, z_w_top: 62)
'''

# ==================================================
# 1. indexing and data load
# ==================================================
Domains={
    "local_wnp": {"lat": (15.0, 45.0), "lon" : (120.0, 180.0)}, # local WNP box
    "trop_wnp": {"lat": (-10.0, 45.0), "lon" : (110.0, 180.0)}, # tropical-WNP connected box
    "north_pacific": {"lat": (20.0, 65.0), "lon" : (110.0, 220.0)}, # broader Pacific box
}

# 파일 정렬 후 파일 잘 잡혔는지
ALL_FILES = sorted(DATA_DIR.glob("*.nc")) 
for f in ALL_FILES:
    print(f)

ref_ds = xr.open_dataset(ALL_FILES[0]) # 기준파일
TLAT_FULL = ref_ds["TLAT"].values # CESM ocean grid의 각 칸이 지구상 어디에 있을까
TLONG_FULL = ref_ds["TLONG"].values # CESM ocean grid의 각 칸이 지구상 어디에 있을까2
ref_ds.close()

# cesm-hr 격자가 1차원 lat/lon이 아니라서 많이 헷갈린다..
''' Example
1D : lat : 0.125, 0.375, 0.625, ...        /  lon : 100.125, 100.375, 100.625 ...
--> ds.sel(lat=slice(...), lon=slice(...))

2D(CESM-HR grid) : curvilinear grid in here.
So, nlat=100, nlon=200이면, TLAT[100, 200]과 TLONG[100, 200]이 어디인지 봐야함.
TLAT_FULL[100, 200] = 27.3
TLONG_FULL[100, 200] = 141.8
--> 격자 인덱스 [100, 200]에 있는 SST 값은 실제 27.3N, 141.8E에 있구나!
'''

def bbox_indices(tlat, tlong, lat_min, lat_max, lon_min, lon_max):
    mask = (
        (tlat >= lat_min) & (tlat <= lat_max) & (tlong >= lon_min) & (tlong <= lon_max)
    )
    rows = np.where(mask.any(axis=1))[0]
    cols = np.where(mask.any(axis=0))[0]
    return slice(int(rows[0]), int(rows[-1]) + 1), slice(int(cols[0]), int(cols[-1]) + 1)

# ==================================================
# 2. compute bounding box indices per domain
# ==================================================
BBOX = {
    name: bbox_indices(TLAT_FULL, TLONG_FULL, *dom["lat"], *dom["lon"])
    for name, dom in Domains.items()
}

# ==================================================
# 3. load, subset, save per domain
# ==================================================
drop_vars = ["z_w", "z_w_bot", "z_w_top", "z_t_150m", "d2"]
ds_full = xr.open_mfdataset(ALL_FILES, 
                            combine="by_coords", 
                            parallel=False, 
                            chunks={"time":1},
                            drop_variables = drop_vars)

for name, (row_sl, col_sl) in BBOX.items():
    sst = ds_full["SST"].isel(nlat=row_sl, nlon=col_sl)
    out_path = OUTPUT_DIR / f"cesm_hr_sst_{name}.nc"
    sst.to_dataset(name="sst").to_netcdf(
        out_path,
        encoding={"sst": {"zlib": True, "complevel": 4, "dtype": "float32"}},
    )
    print(f"저장: {out_path.name}  {dict(sst.sizes)}")

ds_full.close()
