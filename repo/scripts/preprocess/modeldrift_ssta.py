# 0. import and directory
# ============================================================
import xarray as xr
import numpy as np
import nc_time_axis
from pathlib import Path
from scipy.interpolate import LinearNDInterpolator
'''
Source :
https://docs.scipy.org/doc/scipy/reference/generated/scipy.interpolate.LinearNDInterpolator.html
'''
PROC_DIR = Path("../data/processed")
FIG_DIR = Path("../figures")
FIG_DIR.mkdir(parents=True, exist_ok = True)

# Domain local_map, trop_wnp, north_pacific
Domains = ["local_map", "trop_wnp", "north_pacific"]

Clim_start = "1993"
Clim_end = "2024"

# 1. DATA LOAD
# ============================================================
# 1-1. CESM-HR LOAD
# Variables : SST (C), TLAT, TLONG (2D-curve), TIME (cftime)
# ============================================================
cesm = {}
for d in Domains:
    ds = xr.open_dataset(PROC_DIR/f"cesm_hr_sst_domain{d}.nc")
    cesm[d] = ds["SST"]
    print(f"TLAT range : {float(ds.TLAT.min()):.2f} ~ {float(ds.TLAT.max()):.2f}")
    print(f"TLONG range : {float(ds.TLONG.min()):.2f} ~ {float(ds.TLONG.max()):.2f}")
    ds.close()
print('CESM DATA LOADED')

# ============================================================
# 1-2. OISST LOAD
# Variables : sst (C), lat, lon (1D, 0.25), time (datetime64)
# ============================================================
oisst = {}
for d in Domains:
    ds = xr.open_dataset(PROC_DIR / f"oisst_monthly_nwpacific_{d}_1993_2024.nc")
    oisst[d] = ds["sst"]
    print(f"lat range : {float(ds.lat.min()):.3f} ~ {float(ds.lat.max()):.3f}")
    print(f"lon range : {float(ds.lon.min()):.3f} ~ {float(ds.lon.max()):.3f}")
    ds.close()
print('OISST DATA LOADED')

# ============================================================
# 2 CESM-HR Preprocessing
# 2-1 full-record linear detrend 제거 : 격자점별로 1xxx 개월 시계열에 직선을 맞추고 제거 
# 2-1 과정에서 np.polyfit / xr.apply_ufunc 활용 / vectorize = True로 xarray가 공간 격자별로 반복하게 호출해두고, 시간축을 core로 쓰겠음.
# 2-2 monthly climatology 제거 : 선형 추세 제거 후 각 달의 평균을 제거해서 계절 신호 제거
# ============================================================
def linear_detrend(da, dim="time"):
    '''
    da : xr.DataArray - 입력 데이터(with time dimension)
    dim : str
    
    xr.DataArray - 선형 추세 제거 배
    '''
    n = da.sizes[dim]
    