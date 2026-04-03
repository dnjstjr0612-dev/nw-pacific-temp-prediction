"""
CESM-HR SST 다운로드
- OSDF Director가 Range 요청을 지원하지 않아 curl -C - resume 불가
- requests 스트리밍 + 파일별 재시도 루프로 대응
- 완전히 받은 파일(Content-Length 일치)은 skip
"""

import sys
import time
from pathlib import Path
import requests

SAVE_DIR = Path.home() / "Documents/GitHub/Oceanography/nw-pacific-temp-prediction/repo/data/raw/cesm_hr/sst"
SAVE_DIR.mkdir(parents=True, exist_ok=True)

BASE = "https://osdf-director.osg-htc.org/ncar/gdex/d651029/B.E.13.B1850C5.ne120_t12.sehires38.003.sunway_02/ocn/proc/tseries/month_1"

# File 개수 서버 받으면 늘려보기.
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
]

CHUNK   = 4 * 1024 * 1024   # 4 MB 청크
RETRIES = 20                  # 파일당 최대 재시도 횟수
WAIT    = 30                  # 재시도 전 대기 (초)


def download_file(fname: str) -> bool:
    url  = f"{BASE}/{fname}"
    dest = SAVE_DIR / fname
    tmp  = dest.with_suffix(".tmp")

    for attempt in range(1, RETRIES + 1):
        try:
            # 이전 시도에서 받아둔 바이트 수 확인 → Range 요청 시도
            offset = tmp.stat().st_size if tmp.exists() else 0
            headers = {"Range": f"bytes={offset}-"} if offset > 0 else {}

            with requests.get(url, headers=headers, stream=True, timeout=60) as r:
                r.raise_for_status()
                total = int(r.headers.get("Content-Length", 0))

                # 이미 완료된 파일 skip
                if dest.exists() and total and dest.stat().st_size == total:
                    print(f"  skip ({dest.stat().st_size / 1e9:.2f} GB — 완료)")
                    return True

                # 206 Partial Content → 이어받기 / 200 OK → 처음부터
                if r.status_code == 206:
                    received = offset
                    mode = "ab"
                    real_total = offset + total   # Content-Length는 남은 바이트
                    print(f"  resume from {offset/1e6:.0f} MB")
                else:
                    received = 0
                    mode = "wb"
                    real_total = total
                    if offset > 0:
                        print(f"  서버가 resume 미지원 → 처음부터 재시도")

                with open(tmp, mode) as f:
                    for chunk in r.iter_content(chunk_size=CHUNK):
                        f.write(chunk)
                        received += len(chunk)
                        pct = received / real_total * 100 if real_total else 0
                        print(f"\r  {pct:5.1f}%  {received/1e6:8.0f} MB / {real_total/1e9:.2f} GB", end="", flush=True)

            print()
            if real_total and tmp.stat().st_size != real_total:
                raise ValueError(f"크기 불일치: {tmp.stat().st_size} != {real_total}")

            tmp.rename(dest)
            print(f"  완료 ({dest.stat().st_size / 1e9:.2f} GB)")
            return True

        except Exception as e:
            print(f"\n  시도 {attempt}/{RETRIES} 실패: {e}")
            if attempt < RETRIES:
                print(f"  {WAIT}초 후 재시도...")
                time.sleep(WAIT)

    return False


failed = []
for i, fname in enumerate(FILES, 1):
    print(f"\n[{i}/{len(FILES)}] {fname}")
    if not download_file(fname):
        failed.append(fname)

print("\n" + "=" * 60)
if failed:
    print(f"실패 {len(failed)}개:")
    for f in failed: print(f"  {f}")
    sys.exit(1)
print(f"전체 {len(FILES)}개 완료.")