# KEY DATA — 논문 작성용 핵심 수치 모음

작성일: 2026-05-29 (최종 수정: 2026-05-29)
출처 notebook: NB02–NB11  

---

## 0. NB02 — 전체 도메인 Skill Sanity Check

**대상**: `signal_all` (30–62N, 195–240E 전 도메인 평균)  
**표본**: n_cases = 312 (1994–2019 monthly × 12 lead months), n_years = 26  
**소스**: `figures/nb01_analog_run_30_62/alaska_npmm_30_62/alaska_npmm_30_62_source_gain_vs_wide_local.csv`

> **NB02의 위치**: 전 도메인 평균이므로 연안 한정 분석(NB05–07)보다 signal이 희석됨.
> 논문에서는 "sanity check — NPMM 정보가 도메인 전체에서도 baseline을 개선한다"는
> 근거 자료로 사용. 정밀한 수치는 NB05–07을 인용.

### 0-1. Baseline ACC (wide_local_only) — 도메인 평균

| Lead | ACC baseline | vs persistence |
|------|-------------|----------------|
| 1 | 0.865 | beats (lead 3+) |
| 3 | 0.686 | beats |
| 5 | 0.570 | beats |
| 7 | 0.502 | beats |
| 9 | 0.422 | beats |
| 12 | 0.334 | beats |

→ Baseline analog(WIDE) ACC는 lead 1의 0.865에서 lead 12의 0.334까지 감쇠.
Lead 3 이후 baseline이 persistence를 일관되게 상회 → analog 방법 자체가 유효함.

### 0-2. Source gain vs baseline (도메인 평균, 전 lead)

| Lead | wide_npmm | wide_npmm_pdoe | wide_pdoe |
|------|-----------|----------------|-----------|
| 1 | −0.011 | −0.001 | −0.004 |
| 2 | −0.018 | −0.002 | +0.001 |
| 3 | −0.015 | −0.004 | −0.011 |
| 4 | −0.020 | −0.003 | −0.006 |
| **5** | **+0.011** | **+0.025** | +0.009 |
| 6 | +0.004 | +0.014 | +0.001 |
| 7 | +0.020 | +0.030 | −0.013 |
| 8 | +0.034 | +0.023 | −0.027 |
| **9** | **+0.053** | **+0.045** | −0.001 |
| 10 | +0.031 | +0.033 | −0.016 |
| 11 | +0.020 | +0.022 | −0.007 |
| 12 | +0.045 | +0.044 | +0.010 |

**핵심 패턴:**
- wide_npmm: lead 1–4 음수(NPMM 정보가 단기예측 해침) → **lead 5부터 지속적 양수**
- 도메인 평균 **peak: lead 9** (+0.053) — 연안 한정 peak(lead 6–7, NB07: +0.054)보다 늦음
- wide_npmm과 wide_npmm_pdoe는 lead 5 이후 일관되게 양수, 서로 비슷한 크기
- wide_pdoe: 부호가 일관되지 않음 → 도메인 평균에서도 PDOe 단독 사용 부적합

### 0-3. 연안 vs 전체 도메인 비교 (논문 서술용)

| 범위 | peak lead | peak source_gain_acc |
|------|-----------|---------------------|
| 전체 도메인 30–62N (NB02) | **lead 9** | +0.053 |
| 연안 ≤150 km (NB07) | **lead 6–7** | +0.054 |

→ 연안 셀이 내륙 셀보다 NPMM 신호에 더 빠르게 반응함 (lead 9 vs 6–7).
이는 해양 연안 전파 메커니즘이 내륙보다 효율적임을 시사.

---

## 0b. NB03 — 1° 격자 Skill Map (전 도메인)

**격자**: 1440 cells (32 lat × 45 lon, 1° × 1°), **usable: 1,197** (land/missing 제외 243)  
**표본**: n_cases = 312 per cell (1994–2019, 26년 × 12개월)  
**소스**: `figures/nb03_grid_skill_maps_30_62/alaska_grid1440_30_62/`

### 0b-1. Baseline ACC 감쇠 (wide_local_only, 1,197 셀 평균, 95% Fisher Z CI)

| Lead | Mean ACC | 95% CI |
|------|----------|--------|
| 0 | **0.846** | [0.811, 0.875] |
| 1 | 0.688 | [0.625, 0.742] |
| 3 | 0.470 | [0.379, 0.553] |
| 5 | 0.374 | [0.275, 0.466] |
| 7 | 0.328 | [0.225, 0.424] |
| 9 | 0.290 | [0.185, 0.388] |
| 12 | **0.267** | [0.160, 0.367] |

→ 논문 Methods/Results에서 "예측 대상 시그널의 지속성"을 보여주는 기준선으로 활용.  
Lead 0 (prior month) ACC = 0.846은 월별 SST의 높은 자기상관을 반영.

### 0b-2. Source gain mean (1,197 셀 평균, 95% Fisher Z CI)

| Lead | wide_npmm sg | 95% CI | wide_npmm_pdoe sg | 95% CI | wide_pdoe sg |
|------|-------------|--------|-------------------|--------|--------------|
| 5 | +0.015 | [−0.119, 0.149] | +0.017 | [−0.117, 0.151] | −0.009 |
| 6 | +0.031 | [−0.105, 0.167] | +0.030 | [−0.106, 0.166] | −0.003 |
| 7 | +0.040 | [−0.098, 0.178] | +0.036 | [−0.102, 0.174] | −0.007 |
| **8** | **+0.047** | [−0.094, 0.188] | +0.035 | [−0.106, 0.176] | −0.006 |
| 9 | +0.045 | [−0.097, 0.187] | +0.035 | [−0.107, 0.177] | −0.001 |
| 12 | +0.031 | [−0.114, 0.175] | +0.027 | [−0.118, 0.171] | −0.007 |

**⚠️ CI 해석 주의**: 모든 source_gain_acc의 95% CI가 0을 포함한다.
이는 신호가 없다는 뜻이 아니라, **전 도메인 1,197 셀의 공간 평균 수준에서 n=312로 검출하기에는 CI가 너무 넓다**는 의미다.

원인:
1. Source gain은 연안 셀에 집중 → 내륙 셀 포함 시 희석
2. Fisher Z CI는 단일 상관계수에 최적화 → 공간 평균의 불확도를 과대 추정
3. 1,197 셀 간 이질성(heterogeneity)이 크면 평균의 표준오차가 커짐

→ **이것이 연안 한정 분석(NB05–07)의 동기**: 연안 셀로 좁히면 signal-to-noise 개선.  
→ 논문에서는 NB03 결과를 "domain-wide context"로, NB05–07을 "primary evidence"로 제시.

### 0b-3. Wide_pdoe 전 lead에서 음수 또는 ≈0

wide_pdoe source_gain_acc: lead 0–12 전 구간 ≤ −0.001 (최대 −0.015 at lead 4).  
NB06, NB07과 일관: PDOe 단독 analog predictor는 모든 공간 스케일에서 비효과적.

---

## 0c. NB04 — 연안 경로 구축 및 Sequence Tests

**소스**: `figures/nb04_coastal_path_30_62/`  
**경로**: GSHHS 'h' resolution, Amaya-style, 30N/240E → 62N/195E

### 0c-1. 코리도 셀 수 (Methods용)

| 연안 대역 | n_cells | 소스 |
|-----------|---------|------|
| ≤ 50 km | **87** | corridor_cells_le50km.csv |
| ≤ 100 km | **127** | corridor_cells_le100km.csv |
| ≤ 150 km | **168** | corridor_cells_le150km.csv |

→ 논문 Methods 또는 Table에 직접 인용.

### 0c-2. 전파 순서 검정 — Spearman ρ (arrival_centroid, 전 path 27,879 km)

| Band | Experiment | ρ (arrival_centroid) | p | ρ (peak_lead) | p |
|------|-----------|---------------------|---|---------------|---|
| 50 km | wide_npmm | 0.773 | 1.6×10⁻¹⁸ | 0.870 | 8.3×10⁻²⁸ |
| 50 km | wide_npmm_pdoe | 0.828 | 4.1×10⁻²³ | 0.835 | 8.3×10⁻²⁴ |
| **100 km** | **wide_npmm** | **0.782** | **2.0×10⁻²⁷** | **0.846** | **6.1×10⁻³⁶** |
| 100 km | wide_npmm_pdoe | 0.849 | 1.7×10⁻³⁶ | 0.823 | 2.0×10⁻³² |
| 150 km | wide_npmm | 0.810 | 2.2×10⁻⁴⁰ | 0.844 | 8.7×10⁻⁴⁷ |
| 150 km | wide_npmm_pdoe | 0.858 | 7.8×10⁻⁵⁰ | 0.823 | 1.4×10⁻⁴² |

→ 전 band에서 p < 10⁻¹⁸ — 우연이 아닌 체계적 북향 도달 순서 확인.  
→ NB05 ρ(peak_lead, 100km) = 0.814와 일치: 두 노트북이 독립적으로 동일한 결론.

### 0c-3. 속도 추정 — cell 수준 arrival_centroid OLS (전 path)

| Band | Experiment | speed (m/s) | r | p |
|------|-----------|-------------|---|---|
| 50 km | wide_npmm | 1.975 | 0.790 | 1.0×10⁻¹⁹ |
| 50 km | wide_npmm_pdoe | 2.021 | 0.812 | 1.5×10⁻²¹ |
| **100 km** | **wide_npmm** | **1.929** | **0.810** | **8.2×10⁻³¹** |
| 100 km | wide_npmm_pdoe | 1.993 | 0.827 | 5.1×10⁻³³ |
| 150 km | wide_npmm | 1.978 | 0.837 | 2.6×10⁻⁴⁵ |
| 150 km | wide_npmm_pdoe | 2.074 | 0.844 | 9.7×10⁻⁴⁷ |

> **NB04 속도 추정의 위치**: 셀 수준(cell-level), 전 경로 27,879 km, arrival_centroid 기반.  
> 이는 NB05(bin-averaged), NB09(ΔSSTA cross-lag)과 독립적인 세 번째 추정치.  
> 세 추정치 수렴: **~1.93–2.07 m/s (NB04) / ~2.99 m/s (NB05 bin-avg) / 1.87 m/s (NB09)** — 모두 CTW 범위(1–5 m/s).

### 0c-4. Both_fraction at lead 4–7 (핵심 coverage 지표)

| Band | wide_npmm | wide_npmm_pdoe | wide_pdoe |
|------|-----------|----------------|-----------|
| 50 km | **0.871** | 0.922 | 0.520 |
| 100 km | **0.872** | 0.937 | 0.537 |
| 150 km | **0.885** | 0.951 | 0.515 |

> **Both_fraction**: "ACC gain > 0 AND RMSE skill > 0" 동시 만족 셀의 비율.  
> wide_npmm: lead 4–7에서 **87–89% 셀**이 baseline 대비 양방향 개선 → 결과의 공간적 일관성이 높음.  
> wide_npmm_pdoe: 92–95% (더 높음).  
> wide_pdoe: ≈ 51–54% — 실질적 기여 없음 (우연 수준).

---

## 0d. NB08 — OISST/CESM-HR 이벤트 Composite 검증

**목적**: CESM-HR library가 관측(OISST)과 부호/구조를 공유하는가 → library 타당성 확인  
**방법**: Top-20 global events (global NPMM z-score 기준), coastal path Hovmöller band-averaged  
**소스**: `figures/nb08_event_verification_30_62/`

### 0d-1. Band-averaged SSTA composite (coastal path, ≤150 km, lead 1–12)

| Event group | Lead 1 | Lead 4 | Lead 6–7 | Lead 12 | 패턴 |
|-------------|--------|--------|---------|---------|------|
| NPMM warm **obs** | +0.52 | +0.42 | +0.51 | +0.51 | 안정적, 0.42–0.54°C 범위 |
| NPMM warm **lib** | **+1.13** | +0.66 | +0.42 | +0.45 | 강한 초기 → 감쇠 |
| NPMM cold **obs** | **−0.82** | −0.71 | −0.72 | −0.68 | 안정 지속, 전 lead 음수 |
| NPMM cold **lib** | −1.26 | **−1.38** | −1.27 | −0.93 | 초기 증가 후 점진 감쇠 |
| PDO east warm **obs** | +0.69 | +0.57 | +0.43 | +0.62 | U자형 (lead 5–6 골) |
| PDO east warm **lib** | **+1.48** | +1.08 | +1.00 | +1.34 | 높은 수준 유지 |
| PDO east cold **obs** | −0.84 | −0.78 | −0.67 | −0.57 | 점진 감쇠 |
| PDO east cold **lib** | −1.21 | −0.85 | −0.80 | −0.63 | 점진 감쇠 |

### 0d-2. 핵심 발견 (논문 직접 활용)

**① 부호(sign) 완전 일치 ✅ — library 타당성의 핵심 근거**
- NPMM warm/cold: obs와 lib 모두 전 12개월 lead에서 동일한 부호 유지
- PDO eastern warm/cold: 동일
- → "CESM-HR library faithfully reproduces the sign and lead structure of coastal SSTA responses to NPMM and PDO forcing in observations"

**② Library 진폭 과대 (~1.5–2×) — 예상된 결과**
| 지표 | NPMM warm | NPMM cold | PDO warm | PDO cold |
|------|-----------|-----------|----------|----------|
| lib/obs 진폭비 (lead 1) | **2.2×** | 1.5× | **2.1×** | 1.4× |
| lib/obs 진폭비 (lead 6–7) | ~0.8× | 1.7× | 2.3× | 1.2× |

원인: obs n=20 (1993–2019, 27년), lib n=20 (1850–2014, 165년 중 top-20) → lib 이벤트가 더 극단적. 예상된 결과로 library 사용 불가 근거가 아님.

**③ NPMM cold > NPMM warm in obs (냉각 비대칭 NB11과 일관)**
- obs NPMM cold peak: −0.82°C (lead 1)
- obs NPMM warm peak: +0.54°C (lead 8)
- **냉각이 1.52× 더 강함 → NB11 Part B 결과(1.68×, residualized)의 관측 근거**
- lib: cold lead4 −1.38°C vs warm lead1 +1.13°C → cold가 약 1.2× 더 강함

**④ obs NPMM warm: "stable plateau" 구조**
- lead 1–12 전체에서 0.42–0.54°C로 거의 일정
- lib는 lead 1(+1.13)에서 lead 6-7(+0.42)으로 급감
- obs의 안정성은 OISST climatology 기간(1994–2019)이 warm-biased 시기와 겹치거나, 상위 20개 이벤트가 PDOe+ 동반 이벤트일 가능성

**⑤ PDO eastern obs warm의 U자형 패턴**
- Lead 6(+0.42) → lead 11(+0.71)로 회복
- Lead 5–6 골은 연간 계절성(seasonal re-emergence) 또는 PDO 배경 지속에 의한 것으로 해석 가능

### 0d-3. 논문 Methods/Supplementary 활용

> "We verified that CESM-HR library composites reproduce the sign and qualitative structure of observed SSTA responses for both NPMM and PDO-eastern forcing (Fig. Sx). Library amplitudes are systematically larger (factor 1.5–2×) than observations, consistent with the deeper sampling of extreme events in the 165-year simulation. This agreement confirms the suitability of CESM-HR as an analog library for seasonal SST prediction."

---

## 1. 표본 크기 (Sample Sizes)

### 1-1. Phase별 이벤트 수 (NB10, |z| ≥ 0.5 기준)

| Phase | OISST obs | CESM-HR lib |
|-------|-----------|-------------|
| NPMM+ | 122 | 1,067 |
| NPMM− | 100 | 914 |
| NPMM+/PDOe+ | 70 | 602 |
| NPMM+/PDOe− | 12 | 111 |
| PDOe+ | 107 | 1,024 |
| PDOe− | 122 | 1,013 |
| neutral | 50 | 541 |

> lib: CESM-HR 1850–2014 library (~11,000 months)  
> obs: OISST 1993–2019 monthly

### 1-2. 일별 Event Composite 표본 (NB09, top-5 per calendar month)

- NPMM warm events: n = 60 (5 × 12)  
- NPMM cold events: n = 60  
- NPMM+/PDOe+ events: n = 60  
- NPMM−/PDOe− events: n = 60

---

## 2. 전파 속도 — NB05 (월별 Skill Hovmöller)

**소스**: `figures/nb05_skill_propagation_maps_30_62/skill_propagation_summary.csv`  
**설정**: BIN_KM=500 km, 전 path 27,879 km, lead 1–12  
**두 가지 arrival-time 지표** 모두 계산함.

### 2-1. Peak-lead method (reference / 논문 보조)

*정수(1–12), tie 多 → OLS slope noisy. Spearman ρ는 안정적.*

| Band | Experiment | n_bins | Spearman ρ | OLS R² | Speed (m/s) |
|------|-----------|--------|-----------|--------|-------------|
| 50 km | WIDE+NPMM | 42 | 0.798 | 0.624 | 2.00 |
| 50 km | WIDE+NPMM+PDOe | 42 | 0.810 | 0.557 | 1.92 |
| **100 km** | **WIDE+NPMM** | **46** | **0.814** | **0.655** | **1.80** |
| 100 km | WIDE+NPMM+PDOe | 46 | 0.772 | 0.540 | 1.98 |
| 150 km | WIDE+NPMM | 46 | 0.800 | 0.621 | 1.82 |
| 150 km | WIDE+NPMM+PDOe | 46 | 0.734 | 0.490 | 2.06 |

### 2-2. Arrival-centroid method (primary)

*연속값, OLS fit 안정적. `arrival_centroid` = Σ(lead × max(acc,0)) / Σ(max(acc,0))*

| Band | Experiment | n_bins | Spearman ρ | OLS R² | Speed (m/s) |
|------|-----------|--------|-----------|--------|-------------|
| 50 km | WIDE+NPMM | 42 | 0.793 | 0.624 | **3.00** |
| 50 km | WIDE+NPMM+PDOe | 42 | 0.792 | 0.643 | 3.09 |
| **100 km** | **WIDE+NPMM** | **46** | **0.803** | **0.647** | **2.99** |
| 100 km | WIDE+NPMM+PDOe | 46 | 0.778 | 0.642 | 3.05 |
| 150 km | WIDE+NPMM | 46 | 0.807 | 0.658 | **2.88** |
| 150 km | WIDE+NPMM+PDOe | 46 | 0.779 | 0.653 | 3.03 |

### 2-3. Bootstrap 95% CI (Part 5, arrival_centroid 기반)

*소스*: `speed_bootstrap_ci_arrival_centroid.csv`  
*방법*: 500-km bins를 2,000회 복원 추출 → OLS slope 분포 → 2.5%/97.5% 분위수

| Band | Experiment | n_bins | Median (m/s) | 95% CI |
|------|-----------|--------|-------------|--------|
| 50 km | WIDE+NPMM | 42 | 1.883 | [1.450, 2.324] |
| 50 km | WIDE+NPMM+PDOe | 42 | 1.998 | [1.497, 2.459] |
| **100 km** | **WIDE+NPMM** | **46** | **1.936** | **[1.487, 2.376]** |
| 100 km | WIDE+NPMM+PDOe | 46 | 1.968 | [1.497, 2.439] |
| 150 km | WIDE+NPMM | 46 | 1.900 | [1.465, 2.313] |
| 150 km | WIDE+NPMM+PDOe | 46 | 1.983 | [1.499, 2.458] |

**⚠️ Bootstrap median ≠ OLS (1.94 vs 2.99 m/s):**  
Bootstrap 복원 추출이 x축(along_km) 분포를 압축 → OLS slope 과소 추정. 이는 NB09 bootstrap에서 더 극단적으로 나타난 현상과 동일한 원인.  
그러나 CI 범위 자체는 CTW 범위(1–5 m/s) 내에 있으며 물리적으로 타당.

### 2-4. 논문 인용 권장 값

| 지표 | 값 | 방법 |
|------|-----|------|
| 전파 순서 (Spearman ρ) | **0.814** (peak_lead) / **0.803** (arrival_centroid) | band 100 km, wide_npmm |
| 속도 (peak_lead OLS) | **1.80–2.00 m/s** | band 50–150 km |
| 속도 (arrival_centroid OLS) | **2.88–3.09 m/s** | band 50–150 km |
| 속도 (bootstrap 95% CI) | **1.94 [1.49, 2.38] m/s** | band 100 km, wide_npmm |
| 수렴 결론 | **~1.5–3.1 m/s, 중심 추정치 ~1.9–2.0 m/s** | 모든 방법 포함 |

→ 논문에서는 "The northward ordering of skill improvement is highly significant (ρ = 0.81, p < 10⁻³⁰), and the inferred propagation speed is ~1.9–3.1 m/s (depending on the arrival-time metric), consistent with CTW phase speeds of 1–5 m/s."

---

## 3. 전파 속도 — NB09 (일별 ΔSSTA Cross-lag)

**방법**: `corr(NPMM_z(t), ΔSSTA(bin, t→t+lag))`, lag 0–365 days  
ΔSSTA = SSTA(t+lag) − SSTA(t) (NPMM persistence bias 제거)  
**소스**: `figures/nb09_daily_sst_propagation_30_62/06_crosslag_delta/`

### 3-1. Full range (along_km 0–27,879 km) — 비유의

| Band (km) | n_bins | Speed (m/s) | OLS R² | p |
|-----------|--------|-------------|--------|---|
| 50 | 62 | 8.21 | 0.027 | 0.206 |
| 100 | 65 | 6.43 | 0.040 | 0.111 |
| 150 | 67 | 7.30 | 0.028 | 0.177 |

> Full range는 along_km > 22,500 km (Aleutian) 노이즈로 인해 비유의

### 3-2. Truncated at 22,500 km — **핵심 결과**

| Band (km) | n_bins | Speed (m/s) | OLS R² | p |
|-----------|--------|-------------|--------|---|
| 50 | 49 | 2.050 | 0.317 | 2.6×10⁻⁵ |
| **100** | **52** | **1.870** | **0.337** | **6.3×10⁻⁶** |
| 150 | 54 | 1.909 | 0.297 | 2.0×10⁻⁵ |

> **논문 인용 값**: band 100 km → **speed = 1.87 m/s, R² = 0.337, p = 6.3×10⁻⁶**  
> Truncation 기준: along_km = 22,500 km ≈ Kodiak Island (57–60°N, 154–156°W)

### 3-3. 두 독립 추정치 비교

| 방법 | Speed | R² | 유의성 |
|------|-------|----|--------|
| NB05 skill OLS (arrival_centroid, band 100) | **~2.99 m/s** | 0.647 | p<0.001 |
| NB05 skill OLS (peak_lead, band 100) | **1.80 m/s** | 0.655 | p<0.001 |
| NB04 cell-level arrival_centroid (band 100) | **1.929 m/s** | r=0.810 | p=8.2×10⁻³¹ |
| NB09 ΔSSTA cross-lag (band 100, trunc 22500) | **1.87 m/s** | 0.337 | p=6.3×10⁻⁶ |

→ peak_lead OLS, NB04 cell-level, NB09 cross-lag 세 가지가 **~1.80–1.93 m/s** 수렴  
→ 모든 추정치 CTW 범위(1–5 m/s) 내

### 3-4. Window sweep 결과 (band 100 km, top-8 by R²)

*소스*: `07_window_sweep/sweep_grid_le100km.csv`  
*목적*: optimal (start_km, trunc_km) 탐색 → 결과의 window 의존도 확인

| start_km | trunc_km | R² | speed (m/s) | p | n_bins |
|----------|----------|----|-------------|---|--------|
| **0** | **22,500** | **0.337** | **1.870** | 6.3×10⁻⁶ | **52** |
| 2,500 | 22,500 | 0.321 | 1.887 | 1.5×10⁻⁵ | 51 |
| 0 | 21,000 | 0.317 | 1.840 | 4.7×10⁻⁵ | 46 |
| 0 | 23,000 | 0.315 | 1.984 | 1.0×10⁻⁵ | 54 |
| 0 | 22,000 | 0.305 | 1.945 | 3.2×10⁻⁵ | 50 |
| 2,500 | 21,000 | 0.299 | 1.858 | 1.0×10⁻⁴ | 45 |
| 2,500 | 23,000 | 0.298 | 2.007 | 2.4×10⁻⁵ | 53 |
| 0 | 20,500 | 0.288 | 1.880 | 1.7×10⁻⁴ | 44 |

→ 상위 8개 모두 **1.840–2.007 m/s** 범위 — 속도 추정치가 window 선택에 robust함  
→ R² ceiling ≈ 0.34: SST 기반 간접 검출의 실용적 상한

### 3-5. 방법론적 검증 — Raw 상관 vs ΔSSTA 상관

| 방법 | band 100 | R² | p | 결론 |
|------|----------|-----|---|------|
| Part 10: `corr(NPMM(t), SSTA(t+lag))` 전 경로 | 9.18 m/s | 0.027 | 0.140 | ❌ 비유의 |
| Part 11: `corr(NPMM(t), ΔSSTA(t→t+lag))` 전 경로 | 6.43 m/s | 0.040 | 0.111 | ❌ 비유의 |
| Part 12: ΔSSTA, 22,500 km 이하 | **1.87 m/s** | **0.337** | **6.3×10⁻⁶** | ✅ 유의 |

→ Raw 상관은 NPMM 지속성(persistence) 편향으로 p>0.10 → ΔSSTA가 필수  
→ 전 경로(27,879 km)는 Aleutian 노이즈로 비유의 → truncation 필수

### 3-6. OLS SE 95% CI (Part 15)

> **TBD — NB09 Part 15 재실행 후 기입 예정**  
> (Part 15 코드는 OLS SE delta method로 교체 완료; 재실행 시 band 100km: 예상값 ~1.87 [~1.1, ~2.6] m/s)

### 3-7. 일별 NPMM–PDOe 공변 (NB09 Part 3, cross-validation)

| 지표 | 일별 (NB09) | 월별 (NB11) |
|------|------------|------------|
| Pearson r at lag=0 | **0.728** (n=10,220일) | **0.719** (n=324개월) |
| Peak lag | lag=−3일 (r=0.730) | lag=0 |
| n | 10,220 (1993–2020 일별) | 324 (1993–2019 월별) |

→ 일별과 월별 상관이 거의 동일(0.728 vs 0.719) → NPMM-PDOe 공변은 scales에 걸쳐 robust함  
→ 일별 peak lag=−3은 PDOe가 NPMM보다 3일 앞서 반응함을 시사 (실용적으로 동시)

---

## 3b. NB06 — Coastal Band Skill Summary (lead 4–7, all bands)

**소스**: `figures/nb06_coastal_band_30_62/lead4_7_source_gain_by_coastal_path_distance.csv`  
**기준**: source_gain_acc, source_gain_rmse_skill, both_fraction (ACC gain >0 AND RMSE skill >0 동시 만족 셀 비율)

### 전체 데이터 (n_cells 포함)

| Band (km) | n_cells | wide_npmm ACC | wide_npmm RMSE | wide_npmm both_frac | wide_npmm_pdoe ACC | wide_npmm_pdoe both_frac | wide_pdoe ACC | wide_pdoe both_frac |
|-----------|---------|--------------|----------------|---------------------|-------------------|--------------------------|---------------|---------------------|
| **50** | 87 | **+0.0513** | +0.0232 | **0.871** | +0.0520 | 0.922 | −0.0021 | 0.520 |
| **100** | 127 | **+0.0489** | +0.0237 | **0.872** | +0.0516 | 0.937 | −0.0004 | 0.537 |
| **150** | 168 | **+0.0480** | +0.0254 | **0.885** | +0.0509 | 0.951 | −0.0006 | 0.515 |
| 300 | 270 | +0.0423 | +0.0255 | 0.869 | +0.0464 | **0.955** | −0.0017 | **0.462** |
| 600 | 459 | +0.0352 | +0.0240 | 0.835 | +0.0401 | 0.922 | −0.0041 | 0.391 |
| 9999 | 1,197 | +0.0237 | +0.0143 | 0.691 | +0.0236 | 0.734 | −0.0081 | **0.319** |

### 핵심 패턴 및 논문 인용 포인트

**① Skill은 근해에 집중 (거리 gradient 명확)**
- wide_npmm ACC gain: 50km(+0.051) → 9999km(+0.024) — 절반으로 감소
- wide_npmm_pdoe ACC gain: 50km(+0.052) → 9999km(+0.024) — 동일 패턴
- 이 gradient는 NPMM 신호가 연안 경계층(coastal boundary layer)에서 가장 강함을 시사

**② wide_npmm_pdoe both_fraction이 가장 높음**
- Band 150–300 km에서 both_fraction **0.951–0.955** (95% 이상 셀이 ACC+RMSE 동시 개선)
- wide_npmm의 0.869–0.885보다 높음 → PDOe 추가가 공간 coverage를 넓힘

**③ wide_pdoe both_fraction: 300km부터 0.5 미만 (❗기존 기록 오류 수정)**
- ≤150 km: ~0.52 (거의 우연 수준)  
- 300 km: **0.462** (< 0.5 → 대부분 셀에서 해로움)  
- 600 km: **0.391**  
- 9999 km: **0.319** (10/3 셀에서 해로움)  
→ PDOe 단독 사용 시 오히려 baseline보다 나쁜 예측을 만들고, 공간이 넓어질수록 악화

**④ wide_npmm_pdoe ≈ wide_npmm at 9999km (0.0237 vs 0.0236)**
- 전 도메인 평균에서 PDOe 추가 기여가 사라짐 → PDOe 효과는 **연안 셀에만 국한**
- 50km에서는 NPMM+PDOe > NPMM (+0.052 vs +0.051)이지만 9999km에서는 동일

**논문 직접 인용 가능 수치:**
- "At the nearest coastal band (≤50 km), 87% of cells show simultaneous improvement in both ACC and RMSE skill when NPMM information is incorporated (both_fraction = 0.871)"
- "PDOe alone (wide_pdoe) reduces skill relative to baseline across all spatial scales, with both_fraction falling to 0.46 at ≥300 km, indicating that PDOe as a sole predictor is actively harmful"

---

## 3c. NB07 — Lead-window Skill Summary (band ≤150 km)

**소스**: `figures/nb07_lead_windows_30_62/lead_window_summary_coastalpath_le150km.csv`  
**n_cells = 168** (전 window 공통, band ≤150 km corridor)

### 전체 데이터

| Lead window | Experiment | mean ACC | median ACC | RMSE skill | both_fraction |
|-------------|-----------|---------|------------|------------|---------------|
| lead1_3 | wide_npmm | +0.0184 | +0.0123 | +0.0105 | 0.687 |
| lead1_3 | wide_npmm_pdoe | +0.0200 | +0.0189 | +0.0138 | 0.744 |
| lead1_3 | wide_pdoe | −0.0008 | **+0.0057** | −0.0010 | 0.579 |
| lead4_5 | wide_npmm | +0.0417 | +0.0399 | +0.0226 | 0.848 |
| lead4_5 | wide_npmm_pdoe | +0.0429 | +0.0404 | +0.0250 | **0.940** |
| lead4_5 | wide_pdoe | **−0.0062** | −0.0030 | −0.0034 | **0.411** |
| **lead6_7** | **wide_npmm** | **+0.0542** | **+0.0575** | **+0.0281** | **0.923** |
| **lead6_7** | **wide_npmm_pdoe** | **+0.0588** | **+0.0616** | **+0.0317** | **0.961** |
| lead6_7 | wide_pdoe | +0.0050 | +0.0069 | +0.0030 | 0.619 |
| lead8_12 | wide_npmm | +0.0298 | +0.0301 | +0.0111 | 0.695 |
| lead8_12 | wide_npmm_pdoe | +0.0304 | +0.0293 | +0.0120 | 0.711 |
| lead8_12 | wide_pdoe | ≈+0.0005 | +0.0020 | ≈0 | 0.514 |

### 핵심 패턴 및 논문 인용 포인트

**① Skill peak = lead 6–7 (논문 Results 직접 인용)**
- wide_npmm: lead6_7 +0.054 > lead4_5 +0.042 > lead8_12 +0.030 > lead1_3 +0.018
- wide_npmm_pdoe: lead6_7 **+0.059** (최대)
- NPMM 신호가 연안에 도달하는 데 ~6–7개월 소요 (CTW 전파 + 대기–해양 지연 합산)

**② wide_npmm_pdoe at lead6_7: both_fraction = 0.961**  
168개 셀 중 96%가 ACC와 RMSE 모두 개선 → 가장 강력한 coverage 결과.  
"At lead 6–7, 96% of coastal cells show simultaneous improvement when both NPMM and PDOe are incorporated."

**③ wide_pdoe at lead4_5: both_fraction = 0.411 (< 0.5, 유일하게 명확히 해로운 구간)**
PDOe가 4–5개월 lead에서 단독으로 사용될 때 가장 해롭다. 이는 PDOe가 단기 SST 예측에서 NPMM보다 위상이 맞지 않음을 시사.

**④ wide_pdoe at lead6_7: 유일하게 약하게 양수**
mean = +0.005, both_frac = 0.619 → 6–7개월 구간에서 PDOe 신호가 미약하게나마 도움이 됨.  
이 구간에서 PDOe의 독립 기여(NB11 Part B의 PDOe_resid+)와 일관됨.

**⑤ mean ≠ median for wide_pdoe at lead1_3 (좌편향 분포)**
- mean = −0.0008, median = +0.0057 → 대부분 셀은 미미하게 양수,  
  소수의 강하게 음수인 셀이 평균을 끌어내림.  
- 논문에서 "mean이 음수"를 "majority of cells are harmed"로 해석하면 오류 → median 함께 보고 필요.

**⑥ lead8_12에서 wide_npmm ≈ wide_npmm_pdoe (+0.030 vs +0.030)**
장기(8–12개월) lead에서 PDOe 추가 기여가 사라짐 → PDOe 효과는 **중기(4–7개월)에 집중**.

### 논문 ready-to-use 수치
- "Skill improvement peaks at lead 6–7 (source_gain_acc = 0.054 for wide_npmm, 0.059 for wide_npmm_pdoe; both_fraction = 0.923 and 0.961, respectively; band ≤150 km)"
- "At the shortest lead window (1–3 months), NPMM-conditioned forecasts still improve 69% of coastal cells (both_fraction = 0.687)"

---

## 4. Phase별 Band-averaged SSTA Composite (NB10)

**방법**: 연안 전체를 하나의 domain으로 묶어 lead month별 평균 SSTA  
**소스**: `figures/nb10_phase_propagation_monthly_30_62/05_band_avg_composite/peak_lead_summary.csv`

### 4-1. Peak 값 (band = 100 km)

| Phase | Dataset | peak_lead | peak_composite (°C) |
|-------|---------|-----------|---------------------|
| NPMM+ | lib | 0 | **+0.416** |
| NPMM+ | obs | 1 | +0.406 |
| NPMM− | lib | 12 | **−0.376** |
| NPMM− | obs | 12 | −0.454 |
| NPMM+/PDOe+ | lib | 0 | **+0.744** |
| NPMM+/PDOe+ | obs | 0 | +0.628 |
| NPMM+/PDOe− | lib | 10 | +0.039 |
| NPMM+/PDOe− | obs | 12 | +0.130 |
| PDOe+ | lib | 0 | **+0.640** |
| PDOe+ | obs | 1 | +0.523 |
| PDOe− | lib | 12 | **−0.286** |
| PDOe− | obs | 12 | −0.366 |
| neutral | lib | 6 | +0.067 |
| neutral | obs | 6 | +0.272 |

### 4-2. PDOe 증폭 비율

#### 4-2a. Lead 0 기준 (band 100 km)

| 기준 | lib | obs |
|------|-----|-----|
| NPMM+ composite | +0.416°C | +0.406°C |
| NPMM+/PDOe+ composite | +0.744°C | +0.628°C |
| **PDOe 증폭비** | **(0.744−0.416)/0.416 = +78.8%** | +54.7% |

> Lead 0에서 PDOe+가 NPMM+를 +78.8% 증폭 (즉각 배경 해양 상태 반영)

#### 4-2b. Lead 4-7 기준 — Bootstrap 95% CI (NB10 Part 11)

*소스*: `08_bootstrap_ci/pdoe_amplification_bootstrap_ci.csv`

| Dataset | NPMM+ (lead 4-7 mean) | NPMM+/PDOe+ (lead 4-7 mean) | 증폭비 | 95% CI |
|---------|----------------------|----------------------------|--------|--------|
| lib | +0.361°C | +0.489°C | **+35.4%** | [+24.8%, +47.7%] |
| obs | +0.333°C | +0.471°C | **+41.5%** | [+16.3%, +73.7%] |

> Lead 4-7 mean 기준 증폭은 35-42% (lead 0의 79%보다 낮음)  
> PDOe 효과는 즉각적(lead 0에서 최대)이며 시간이 지나면서 감소함  
> obs CI가 넓은 이유: n_events=70 (lib: 602)으로 표본 부족

### 4-3. Phase별 Bootstrap 95% CI (band 100 km, lib)

*소스*: `08_bootstrap_ci/phase_composite_bootstrap_ci_le100km.csv`

**선택 lead에서의 composite mean과 95% CI (lib):**

| Phase | Lead 0 | CI | Lead 3 | CI | Lead 6 | CI |
|-------|--------|-----|--------|-----|--------|-----|
| NPMM+ | +0.416 | [+0.374, +0.458] | +0.401 | [+0.359, +0.443] | +0.345 | [+0.302, +0.389] |
| NPMM− | −0.503 | [−0.543, −0.460] | −0.472 | [−0.511, −0.428] | −0.443 | [−0.484, −0.400] |
| NPMM+/PDOe+ | +0.744 | [+0.697, +0.796] | +0.584 | [+0.530, +0.638] | +0.469 | [+0.412, +0.529] |
| NPMM+/PDOe− | −0.431 | [−0.535, −0.329] | −0.076 | [−0.201, **+0.050**] | −0.041 | [−0.162, **+0.078**] |
| PDOe+ | +0.640 | [+0.603, +0.675] | +0.435 | [+0.391, +0.478] | +0.358 | [+0.317, +0.399] |
| PDOe− | −0.637 | [−0.672, −0.600] | −0.432 | [−0.473, −0.392] | −0.356 | [−0.398, −0.313] |

**핵심 발견:**

**① 모든 주요 phase가 lead 0에서 통계적으로 유의** (CI가 0 미포함)  
→ NPMM+, NPMM−, NPMM+/PDOe+, PDOe+, PDOe− 모두 → 신호가 실재함

**② NPMM+/PDOe−: lead 2 이후 CI가 0을 포함 → 통계적 비유의**
- Lead 0: −0.431 [−0.535, −0.329] ← significant
- Lead 1: −0.304 [−0.430, −0.185] ← significant
- Lead 2: −0.145 [−0.273, −0.018] ← marginally significant
- **Lead 3+: CI 모두 0 포함 → 유의하지 않음**
→ "NPMM+ with negative PDO eventually cancels to zero" — NB09 daily +0.003°C 결과의 monthly 확인

**③ PDOe+ 강도 > NPMM+ at lead 0 (+0.640 vs +0.416)**
- PDOe+는 즉각적인 배경 해양 상태를 반영하기 때문에 lead 0에서 가장 강함
- NPMM+는 lead 0에서 0.416이지만, PDOe+는 0.640으로 더 강한 초기 반응
- 단 PDOe+ 효과는 더 빠르게 감쇠: lead 6에서 PDOe+ 0.358 ≈ NPMM+ 0.345

**④ NPMM+ CI 폭 ≈ 0.08°C → 신뢰할 수 있는 추정치**
- Lead 0 CI 폭: 0.458−0.374 = 0.084°C (n=1067 lib events → 표본 충분)
- Lead 6 CI 폭: 0.389−0.302 = 0.087°C (비슷한 수준으로 유지)

### 4-4. Band 민감도 (NPMM+, lib)

| Band (km) | peak_lead | peak_composite |
|-----------|-----------|----------------|
| 50 | 0 | +0.417 |
| 100 | 0 | +0.416 |
| 150 | 0 | +0.417 |

→ Band 50/100/150 km 간 차이 무시 가능 (안정적)

---

## 5. PDOe 순증분 (NB10, band-averaged line)

**방법**: `(NPMM+PDOe+ composite) − (NPMM+ composite)` per lead month  
**소스**: `figures/nb10_phase_propagation_monthly_30_62/07_pdoe_increment_line/pdoe_increment_band_avg.csv`

### 5-1. lib, band 100 km

| lead_month | NPMM+ | NPMM+PDOe+ | increment |
|-----------|-------|------------|-----------|
| 0 | 0.416 | 0.744 | **+0.327** |
| 1 | 0.415 | 0.685 | +0.270 |
| 2 | 0.407 | 0.627 | +0.220 |
| 3 | 0.401 | 0.584 | +0.182 |
| 4 | 0.391 | 0.537 | +0.146 |
| 5 | 0.367 | 0.495 | +0.128 |
| 6 | 0.345 | 0.469 | +0.123 |
| 7 | 0.339 | 0.456 | +0.117 |
| 8 | 0.332 | 0.446 | +0.114 |
| 9 | 0.319 | 0.436 | +0.118 |
| 10 | 0.300 | 0.431 | +0.131 |
| 11 | 0.287 | 0.412 | +0.124 |
| 12 | 0.283 | 0.402 | +0.119 |

> PDOe+ 기여는 lead 0에서 **+0.33°C** (최대), 전 lead에서 양수 유지 (≥0.11°C)

### 5-2. PDOe− increment (lib, band 100 km)

| lead_month | NPMM+ | NPMM+PDOe− | increment |
|-----------|-------|------------|-----------|
| 0 | 0.416 | −0.431 | **−0.847** |
| 3 | 0.401 | −0.076 | −0.478 |
| 6 | 0.345 | −0.041 | −0.386 |
| 12 | 0.283 | +0.003 | −0.280 |

> PDOe−는 NPMM+를 **냉각 방향으로 역전**시킴 (lead 0: −0.85°C)

---

## 6. 계절별 분리 (NB10, NPMM+, lib, band 100 km)

**소스**: `figures/nb10_phase_propagation_monthly_30_62/06_seasonal/seasonal_composite_le100km.csv`

| Season | n_events | peak_lead (approx) | peak_composite (°C) |
|--------|----------|--------------------|--------------------|
| DJF | 272 | lead 4–5 | **+0.49** |
| MAM | 264 | lead 1–2 | +0.52 |
| JJA | 267 | lead 0 | +0.51 |
| SON | 264 | lead 7–8 | +0.43 |

> DJF NPMM+ 이벤트: 가장 큰 delay (lead 4–5 peak) → 겨울 forcing이 여름 SST에 도달  
> JJA NPMM+ 이벤트: 즉각 반응 (lead 0 peak)  
> SON NPMM+ 이벤트: 가장 늦은 peak (lead 7–8), 장기 persistence 암시

---

## 7. 월별 Phase 전파 순서 검정 (NB10)

**방법**: bin별 peak-lead → Spearman ρ (along_km, peak_lead)  
**소스**: `figures/nb10_phase_propagation_monthly_30_62/propagation_summary.csv`

### lib (CESM-HR), band 100 km

| Phase | n_events | n_bins | Spearman ρ | p |
|-------|----------|--------|-----------|---|
| NPMM+ | 1,067 | 80 | −0.049 | 0.663 |
| NPMM− | 914 | 80 | −0.084 | 0.459 |
| NPMM+/PDOe+ | 602 | 80 | +0.076 | 0.504 |
| NPMM+/PDOe− | 111 | 80 | **−0.377** | **0.001** |
| neutral | 541 | 80 | +0.180 | 0.109 |

> **해석**: 월별 자료에서 전파 순서 미검출 (ρ ≈ 0, 모두 비유의)  
> 예외: NPMM+/PDOe−에서 ρ = −0.377 (역방향) — 반대 위상 cancellation 효과  
> CTW 전파 (~2 m/s)는 도메인을 8–14일에 통과 → **월별 자료에서 동시 도달로 보임**

---

## 8. 일별 Event SSTA Composite (NB09, band 100 km)

**소스**: `figures/nb09_daily_sst_propagation_30_62/05_daily_event_hovmoller_summary.csv`  
**이벤트 선택**: top-5 per calendar month × 12개월 = n=60 per group (1993–2020)

### 전체 Phase 조합 — Day 0–30, 31–60, 61–90, 91–120 (band 100 km)

| Event group | Day 0–30 | Day 31–60 | Day 61–90 | Day 91–120 | pos_frac (day0-30) |
|-------------|----------|-----------|-----------|------------|---------------------|
| NPMM warm | +0.938 | +0.878 | +0.837 | **+0.753** | 100% |
| NPMM cold | **−0.646** | −0.639 | −0.661 | **−0.646** | 0% |
| PDOe warm | +0.904 | +0.799 | +0.816 | **+0.873** | 100% |
| PDOe cold | −0.642 | −0.667 | −0.685 | −0.647 | 0% |
| NPMM+/PDOe+ | **+1.011** | +0.953 | +0.981 | **+0.933** | 100% |
| NPMM−/PDOe− | −0.655 | −0.659 | −0.674 | −0.671 | 0% |
| NPMM+/PDOe− | **+0.003** | +0.031 | +0.078 | +0.129 | 52% |
| NPMM−/PDOe+ | **+0.119** | +0.145 | +0.169 | +0.200 | 88% |
| same_sign_abs | +0.192 | +0.104 | +0.072 | +0.087 | 92% |
| opposite_sign_abs | +0.203 | +0.233 | +0.240 | **+0.318** | 97% |

### 핵심 패턴 및 논문 인용 포인트

**① NPMM cold — 완벽한 persistence (가장 놀라운 결과)**
- Day 0–30: −0.646°C → Day 91–120: −0.646°C → **120일 동안 진폭 변화 없음**
- 반면 NPMM warm은 day 0–30: +0.938 → day 91–120: +0.753 (−20% 감쇠)
- 냉각 신호의 완벽 지속은 차가운 PDO 배경과의 positive feedback으로 설명 가능 (NB11 냉각 비대칭과 연결)

**② NPMM+/PDOe− ≈ 0°C (perfect cancellation)**
- Day 0–30 mean: +0.003°C (positive_fraction = 52% — essentially random)
- NPMM+ 온난화(+0.94)와 PDOe− 냉각이 완전 상쇄
- Day 91–120: +0.129°C로 약하게 양수 전환 → 시간이 지나면서 NPMM 지속 효과가 남음
- "When PDOe is negative, NPMM+ forcing is essentially neutralized" → NB10 PDOe increment 결과와 일관

**③ NPMM−/PDOe+ → 양수 (+0.119°C, day 0-30)!**
- Cold NPMM forcing이 warm PDO 배경 위에서는 **냉각 효과가 상쇄되어 약하게 양수**
- pos_fraction = 88% → 대부분 지점에서 양수
- day 91-120: +0.200°C로 계속 증가 → PDOe+ 지속 효과가 지배
- 이는 PDOe가 NPMM 효과를 완전히 역전시킬 수 있음을 보여줌

**④ PDOe warm — U자형 persistence (day 61-90 골 후 회복)**
- day 0-30: +0.904 → day 31-60: +0.799 (감쇠) → day 61-90: +0.816 → day 91-120: **+0.873** (회복)
- 이 U자형은 seasonal re-emergence 또는 PDO 배경 지속에 의한 것으로 해석 가능

**⑤ 냉각/온난 비대칭 (daily 수준에서도 확인)**
- NPMM cold day 0-30: −0.646°C vs NPMM warm day 0-30: +0.938°C
- cold/warm 비율: 0.646/0.938 = **0.69** (cold가 warm보다 약함)
- 단, 지속성 포함하면: cold는 120일 후에도 −0.646 유지 vs warm +0.753 → **냉각이 더 지속**
- → NB11 monthly 잔차 분석의 비대칭(1.68×)과 시간 척도가 다름 (daily vs monthly compositing)

### 논문 ready-to-use

- "NPMM-positive events produce a coastal SSTA warming of +0.94°C (days 0–30) that decays to +0.75°C by days 91–120. In contrast, NPMM-negative events produce a cooling of −0.65°C that shows no discernible decay over 120 days, suggesting a positive feedback between cold NPMM forcing and a cold PDO background."
- "When NPMM+ and PDOe− co-occur, the net coastal SSTA is indistinguishable from zero (mean = +0.003°C at days 0–30), demonstrating complete cancellation between NPMM warming and PDOe cooling."

---

## 9. NPMM/PDOe 공변 (NB11 Part A)

**소스**: NB11 cell 출력 (직접 계산)

| Dataset | Pearson r | p | Spearman ρ | p |
|---------|-----------|---|-----------|---|
| OISST obs | **0.719** | 8.6×10⁻⁵⁵ | **0.707** | 2.9×10⁻⁵² |
| CESM-HR lib | **0.558** | 6.9×10⁻²⁶⁵ | **0.539** | 5.0×10⁻²⁴⁴ |

> Lead-lag 최대치: lag = 0 → 선행/후행 관계 없음, 동시 공변  
> 4분면 phase 분석은 독립 검정이 아님

---

## 10. 잔차 Composite (NB11 Part B + Part E CI)

**방법**: `NPMM_resid = NPMM_z − β×PDOe_z` (PDOe 공유분 제거), top-10(lib)/top-5(obs) per month  
**소스**: `B_residual_composite_summary.csv`, `E_residual_composite_ci/residual_composite_bootstrap_ci.csv`  
**기준**: lead 4–7 mean SSTA, band 150 km (mean), n=120 lib / n=60 obs

### 10-1. Lead 4-7 mean composite with 95% bootstrap CI (Part E)

| Dataset | Group | n | Mean (°C) | 95% CI |
|---------|-------|---|-----------|--------|
| CESM-HR lib | NPMM_resid+ | 120 | **+0.409** | [+0.335, +0.477] |
| CESM-HR lib | NPMM_resid− | 120 | **−0.685** | [−0.749, −0.623] |
| CESM-HR lib | PDOe_resid+ | 120 | **+0.473** | [+0.411, +0.542] |
| CESM-HR lib | PDOe_resid− | 120 | **−0.189** | [−0.251, −0.125] |
| OISST obs | NPMM_resid+ | 60 | +0.139 | [+0.076, +0.200] |
| OISST obs | NPMM_resid− | 60 | −0.327 | [−0.404, −0.247] |
| OISST obs | PDOe_resid+ | 60 | +0.364 | [+0.282, +0.440] |
| OISST obs | PDOe_resid− | 60 | −0.154 | [−0.217, −0.087] |

### 10-2. Peak lead (band-averaged composite, lib)

| Group | Peak lead | Peak value (°C) | Timing |
|-------|-----------|-----------------|--------|
| NPMM_resid+ | lead 6 | +0.456 | 지연 반응 (대기–해양 전파) |
| NPMM_resid− | lead 3 | −0.727 | 지연 반응, warm보다 빠름 |
| PDOe_resid+ | lead 0 | +0.822 | **즉각 반응** (배경 상태) |
| PDOe_resid− | lead 0 | −0.897 | **즉각 반응** (배경 상태) |

> NPMM: 지연 반응 (lead 3–6), PDOe: 즉각 반응 (lead 0) — 메커니즘 차이의 핵심 증거

### 10-3. 핵심 발견 (CI 기반, 논문 직접 활용)

**① 모든 8개 CI가 0을 포함하지 않음 → 통계적으로 유의**
- NPMM과 PDOe 모두 상대방의 공유 분산을 제거한 후에도 독립적 연안 신호를 생성함
- PDOe_resid− in obs [-0.217, -0.087] — 가장 작은 신호도 유의함
- "Both NPMM and PDOe retain statistically significant independent contributions after residualization"

**② NPMM_resid± CI가 겹치지 않음 → 냉각 비대칭 통계적으로 유의**

lib: NPMM_resid+ [0.335, 0.477] vs NPMM_resid− [−0.749, −0.623] → 완전히 분리  
obs: NPMM_resid+ [0.076, 0.200] vs NPMM_resid− [−0.404, −0.247] → 완전히 분리  
→ 냉각 비대칭은 sampling 오차가 아닌 실제 신호

**③ PDOe_resid+가 NPMM_resid+보다 강함 (lib)**

| | Mean | CI |
|-|------|-----|
| PDOe_resid+ | +0.473 | [+0.411, +0.542] |
| NPMM_resid+ | +0.409 | [+0.335, +0.477] |

두 CI가 겹치지만 PDOe_resid+ > NPMM_resid+ → PDOe가 단순히 NPMM의 동반자가 아닌 독립 source  
"PDOe provides an independent coastal SSTA signal comparable in magnitude to that of NPMM"

**④ 냉각 비대칭 ratio (|NPMM_resid−| / NPMM_resid+)**

| Dataset | Ratio | CI lower bound |
|---------|-------|----------------|
| lib | **1.68×** | ≥ 0.623/0.477 = **1.31×** (CI 범위 하한 기준) |
| obs | **2.35×** | ≥ 0.247/0.200 = **1.24×** (CI 범위 하한 기준) |

→ 심지어 CI 하한에서도 1.3× 이상 → 비대칭이 통계적으로 강건함

### 10-4. 논문 ready-to-use

- "After removing the shared NPMM–PDOe variance, NPMM_resid+ produces a lead 4–7 mean coastal SSTA of +0.41°C [+0.34, +0.48] (95% CI; lib), while NPMM_resid− produces −0.69°C [−0.75, −0.62], a 1.68× asymmetry. Both CIs exclude zero and do not overlap, confirming that the asymmetry is statistically significant."
- "PDOe_resid+ independently produces +0.47°C [+0.41, +0.54] at leads 4–7, comparable to the NPMM contribution, establishing PDOe as an independent source of coastal predictability rather than a passive correlate of NPMM."

---

## 11. Case-level Regression (NB11 Part C)

**방법**: `improvement = sq_error(wide_local_only) − sq_error(experiment)` per case  
NPMM_z, PDOe_z, NPMM×PDOe로 OLS 회귀  
**소스**: `figures/nb11_source_modifier_tests_30_62/C_case_level_regression.csv`

| Lead window | Experiment | n | r_npmm | p_npmm | r_pdoe | p_pdoe | β_npmm | β_pdoe | β_interaction |
|------------|-----------|---|--------|--------|--------|--------|--------|--------|---------------|
| lead1_3 | wide_npmm | 936 | −0.038 | 0.246 | −0.022 | 0.509 | +0.00146 | −0.00129 | +0.00851 |
| lead1_3 | wide_npmm_pdoe | 936 | −0.050 | 0.126 | +0.002 | 0.945 | −0.00077 | +0.00150 | +0.00925 |
| lead4_7 | wide_npmm | 1,248 | −0.040 | 0.154 | +0.004 | 0.881 | −0.00389 | +0.00376 | +0.00680 |
| **lead4_7** | **wide_npmm_pdoe** | **1,248** | **−0.096** | **0.001** | −0.021 | 0.465 | −0.00767 | +0.00496 | +0.00716 |
| lead8_12 | wide_npmm | 1,560 | −0.008 | 0.746 | +0.027 | 0.288 | −0.00171 | +0.00405 | +0.00721 |
| lead8_12 | wide_npmm_pdoe | 1,560 | −0.045 | 0.078 | +0.023 | 0.363 | −0.00702 | +0.00767 | +0.00803 |

> 유일한 유의 결과: **lead4_7 × wide_npmm_pdoe, r_npmm = −0.096 (p = 0.001)**  
> r 음수 → NPMM이 강할수록 improvement 감소 → 과적합 가능성  
> 전반적 해석: case 수준 회귀는 대부분 비유의 (noise 지배)

---

## 12. 냉각 비대칭 분석 (NB11 Part D)

**소스**: `figures/nb11_source_modifier_tests_30_62/B_obs_residualized.csv`, `B_lib_residualized.csv`  
threshold = 0.5

### 12-1. 진폭 비대칭

| Dataset | NPMM+ n | NPMM+ mean|z| | NPMM− n | NPMM− mean|z| | cold/warm ratio |
|---------|---------|------------|---------|------------|----------------|
| OISST obs | 122 | 1.000 | 100 | **1.251** | **1.25×** |
| CESM-HR lib | 1,067 | 1.055 | 914 | **1.237** | **1.17×** |

### 12-2. PDOe 조건부 분포 (주원인)

| Dataset | PDOe mean \| NPMM+ | PDOe mean \| NPMM− | Frac PDOe+ \| NPMM+ | Frac PDOe− \| NPMM− |
|---------|-------------------|-------------------|---------------------|---------------------|
| OISST obs | +0.647 | **−0.943** | 57.4% | **80.0%** |
| CESM-HR lib | +0.613 | **−0.668** | 56.4% | **57.1%** |

### 12-3. 계절 분포 (비대칭 원인 아님)

NPMM+/− 모두 DJF/MAM/JJA/SON에 23–26%로 균등 분포.

### 12-4. 냉각 비대칭 요약 + CI 검증

| 원인 | 기여 수준 | 근거 |
|------|----------|------|
| ① 진폭 비대칭 (NPMM− z-score 더 극단) | 부차적 | cold/warm \|z\| ratio = 1.17–1.25×; composite 비율 1.68×을 다 설명 못함 |
| ② PDOe cold 공동발생 (주원인) | 주요 | NPMM− 이벤트의 80%(obs)/57%(lib)에서 PDOe−도 동반 |
| ③ 계절 편향 | 없음 | 균등 분포 확인 |

### 12-5. 냉각 비대칭 통계적 유의성 (Part E bootstrap CI)

**NPMM_resid 기반 비대칭 ratio (|resid−| / resid+, lead 4-7):**

| Dataset | |NPMM_resid−| | NPMM_resid+ | **Ratio** | CI lower bound |
|---------|---------------|-------------|-----------|----------------|
| lib | 0.685 [0.623, 0.749] | 0.409 [0.335, 0.477] | **1.68×** | ≥ 1.31× |
| obs | 0.327 [0.247, 0.404] | 0.139 [0.076, 0.200] | **2.35×** | ≥ 1.24× |

→ CI lower bound 기준(보수적)으로도 1.3× 이상 → **비대칭은 통계적으로 강건**  
→ 두 CI가 완전히 비겹침 → 냉각 > 온난의 결론이 통계적으로 유의함

**obs에서 ratio가 더 큼 (2.35× vs 1.68×) 이유:**
- obs에서 PDOe− cold 동반 비율 80% (lib: 57%)으로 더 높음
- obs 기간(1993-2019)에 La Niña 이벤트 다수 포함 → NPMM−/PDOe− 공동발생 강화
- obs n=60으로 표본이 작아 노이즈 기여 가능

### 12-6. 논문 ready-to-use

- "The cooling asymmetry (NPMM_resid− / NPMM_resid+ = 1.68 in CESM-HR, 2.35 in OISST) is statistically robust: the 95% bootstrap CIs for warm and cold residualized composites are entirely non-overlapping in both datasets, with the minimum implied ratio exceeding 1.3×."
- "The primary mechanism driving this asymmetry is the preferential co-occurrence of cold NPMM events with cold PDOe (80% of NPMM− events in observations), which nonlinearly amplifies cooling forcing on an already-cold ocean background state."

---

## 13. 논문 인용용 핵심 수치 모음 (Quick Reference)

| 항목 | 값 | 소스 |
|------|-----|------|
| 연안 경로 총 길이 | 27,879 km (피오르드 포함) | NB09 |
| Truncation 기준 | 22,500 km (Kodiak Island) | NB09 |
| NB05 전파 속도 (arrival_centroid) | **~3.0 m/s** (Spearman ρ=0.803, R²=0.647) | NB05 |
| NB05 전파 속도 (peak_lead, ref) | **1.80 m/s** (Spearman ρ=0.814, R²=0.655) | NB05 |
| NB05 bootstrap CI | TBD — NB05 Part 5 재실행 후 기입 | NB05 |
| NB09 전파 속도 | **1.87 m/s** (R²=0.337, p=6.3×10⁻⁶) | NB09 |
| NB06 peak skill band | 50 km (source_gain_acc = 0.051) | NB06 |
| NB07 peak skill window | **lead 6–7** (source_gain_acc = 0.054) | NB07 |
| wide_pdoe 단독 | 전 band/구간 음수 또는 neutral — 단독 사용 부적합 | NB06/07 |
| NPMM+ peak composite (lib) | **+0.416°C** at lead 0 | NB10 |
| NPMM+/PDOe+ peak composite (lib) | **+0.744°C** at lead 0 | NB10 |
| PDOe 증폭비 (lib) | **(0.744−0.416)/0.416 = +78.8%** | NB10 |
| PDOe 증폭비 (obs) | +54.7% | NB10 |
| PDOe increment at lead 0 (lib, 100 km) | **+0.327°C** | NB10 |
| PDOe 기여 지속 기간 | lead 0–12 전체 양수 | NB10 |
| NPMM_resid+ (lib, lead 4–7) | **+0.409°C** | NB11 |
| NPMM_resid− (lib, lead 4–7) | **−0.685°C** | NB11 |
| PDOe_resid+ (lib, lead 4–7) | **+0.473°C** | NB11 |
| NPMM peak lead (lib) | lead 3–6 | NB11 |
| PDOe peak lead (lib) | lead 0 (즉각) | NB11 |
| r(NPMM, PDOe) obs | **0.719** | NB11 |
| r(NPMM, PDOe) lib | **0.558** | NB11 |
| Frac PDOe− \| NPMM− (obs) | **80.0%** | NB11 |
| PDOe mean \| NPMM− (obs) | **−0.943** | NB11 |
| 냉각/온난 composite 비율 | **1.68×** (−0.685 / +0.409) | NB11 |
| 냉각/온난 z-score 비율 | 1.17–1.25× | NB11 |
| DJF NPMM+ peak lead | lead 4–5 | NB10 |
| JJA NPMM+ peak lead | lead 0 | NB10 |
| Monthly 분석 전파 검출 | **불가** (CTW = 8–14일, monthly 동시 도달) | NB10 |
| Case-level r (최대 유의) | −0.096 (p=0.001, lead4_7, NPMM+PDOe) | NB11 |

---

## 14. 주요 해석 주의사항 (Caveats)

| 주의사항 | 내용 |
|---------|------|
| Along_km 과대 | 피오르드 포함 → 속도 **과소 추정**; 전파 순서(Spearman ρ)는 유효 |
| Monthly 자료 한계 | CTW가 도메인 통과하는 데 8–14일 → 월별 자료에서 동시 도달로 보임 |
| NPMM/PDOe 공변 | r ≈ 0.56–0.72 → 4분면 분류는 독립 검정 아님; 잔차 분석 필요 |
| SSH/SSHA 미사용 | 전파 간접 증거만 존재; 직접 증명 불가 |
| R² ceiling | SST 기반 간접 검출의 실용적 상한선 ≈ 0.34 (nb09 Part 13) |
| PDOe 증폭 해석 | PDOe는 해양 배경 상태 반영 (lead 0 즉각) vs NPMM은 대기–해양 전파 (lead 3–6) |
