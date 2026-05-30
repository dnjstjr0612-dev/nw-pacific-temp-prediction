# Analog-Based Seasonal Prediction of Coastal Sea Surface Temperature along the Northeast Pacific: Roles of the North Pacific Meridional Mode, Pacific Decadal Oscillation, and SST-Based Coastal Signal Propagation

## Manuscript Framing

This manuscript is framed as a *Journal of Climate*-style process-and-predictability study. The central result is that NPMM-conditioned analogs improve northeast Pacific coastal SSTA prediction most clearly in the coastal band and at forecast leads 6–7 months, while PDOe alone is not a reliable analog predictor. PDOe is instead treated as a pre-existing coastal-ocean background-state modifier that amplifies or suppresses NPMM-related coastal SSTA. The propagation evidence is deliberately conservative: SST-based skill and ΔSSTA diagnostics provide strong indirect evidence for northward coastal signal ordering with speeds compatible with CTW/coastal Kelvin wave dynamics, but direct confirmation requires SSH/SSHA or subsurface temperature analysis. Quantitative values follow `KEY_DATA.md`, methodological guardrails follow `README_RUN_ORDER.md`, the scientific logic follows `RESEARCH_SUMMARY.md`, and the structure is revised from `DRAFT_PAPER.md`.

---

## Abstract

Seasonal prediction of coastal sea surface temperature anomalies (SSTA) along the northeast Pacific and Alaska coast remains challenging beyond persistence timescales. We evaluate an analog-based forecasting framework that uses OISST observations and a high-resolution CESM-HR analog library to test whether the North Pacific Meridional Mode (NPMM) and the extratropical Pacific Decadal Oscillation component (PDOe) improve seasonal coastal SSTA prediction relative to a broad-domain SST-pattern baseline. Forecast-period results are evaluated only from lead 1 onward because lead 0 represents the prior-month state, not a forecast month. The baseline `wide_local_only` analog forecast is skillful, with domain-wide ACC decreasing from 0.688 at lead 1 to 0.267 at lead 12 and exceeding persistence from lead 3 onward. Domain-wide NPMM source gain is negative or near-zero at leads 1–2 but becomes positive from lead 3 onward, whereas the coastal-band response peaks earlier and more sharply.

The primary forecast-skill improvement is coastal. In the ≤150 km coastal band, NPMM-conditioned analogs reach their maximum improvement at lead 6–7, with `source_gain_acc = +0.0542`, RMSE skill `= +0.0281`, and `both_fraction = 0.923`. Adding PDOe increases the lead 6–7 improvement to `source_gain_acc = +0.0588`, RMSE skill `= +0.0317`, and `both_fraction = 0.961`. Skill decreases offshore, indicating that the NPMM-related predictable signal is concentrated near the coastal boundary. PDOe alone is generally unsuitable as a sole analog predictor: at lead 4–5 in the ≤150 km coastal band, `wide_pdoe` gives `source_gain_acc = −0.0062` and `both_fraction = 0.411`, and its spatial coverage falls below 0.5 at ≥300 km.

SST-based propagation diagnostics provide strong indirect evidence for northward ordering of the coastal signal. Three independent estimates cluster near 1.80–1.93 m s⁻¹: NB05 peak-lead OLS gives 1.80 m s⁻¹, NB04 cell-level arrival_centroid OLS gives 1.929 m s⁻¹, and NB09 daily ΔSSTA cross-lag analysis gives 1.87 m s⁻¹ with 95% CI [1.13, 2.61] m s⁻¹. These values are compatible with broad CTW/coastal Kelvin wave speeds, but they do not constitute direct wave detection. Direct confirmation requires SSH/SSHA or subsurface temperature analysis. Monthly composites cannot resolve this propagation because a ~2 m s⁻¹ signal would cross the analyzed coastal domain in roughly 8–14 days and therefore appear nearly simultaneous at monthly resolution.

PDOe acts as a pre-existing background-state modifier, whereas NPMM produces a delayed forecast-period response. The primary PDOe amplification metric is the lead 4–7 mean: NPMM+/PDOe+ amplifies NPMM+ coastal SSTA by +35.4% in CESM-HR, with 95% CI [+24.8%, +47.7%], and by +41.5% in OISST, with 95% CI [+16.3%, +73.7%]. Lead-1 amplification is larger, +65.1% in CESM-HR and +51.1% in OISST, and is used only as an early forecast-period reference. Because NPMM and PDOe are strongly correlated, with monthly Pearson r = 0.719 in OISST and r = 0.558 in CESM-HR, residual analysis is required. Lead 4–7 residual composites show statistically significant independent coastal SSTA signals for both indices, including CESM-HR NPMM_resid+ = +0.409°C, NPMM_resid− = −0.685°C, PDOe_resid+ = +0.473°C, and PDOe_resid− = −0.189°C, with all 95% bootstrap confidence intervals excluding zero.

A robust cooling asymmetry is also found. NPMM_resid− cooling exceeds NPMM_resid+ warming by 1.68× in CESM-HR and 2.35× in OISST. This asymmetry is consistent with PDOe-modulated nonlinear amplification: NPMM− events have slightly larger |z| values than NPMM+ events, but the dominant factor is preferential co-occurrence of cold NPMM with cold PDOe, including 80.0% co-occurrence in observations and mean PDOe given NPMM− of −0.943. These results indicate that NPMM supplies the primary delayed coastal forecast pathway, while PDOe modulates the background state on which that pathway acts.

---

## 1. Introduction

Seasonal prediction of coastal SST along the northeast Pacific and Alaska coast is a scientifically and societally important problem. Coastal SSTA influence marine heatwaves, ecosystem productivity, fisheries-relevant habitat, and biogeochemical variability. The northeast Pacific coastal zone is also dynamically complex because local air–sea fluxes, basin-scale atmospheric teleconnections, boundary currents, shelf processes, and remote wave dynamics can all affect the coastal SST state. A central challenge is therefore not only to improve seasonal forecast skill, but also to identify which large-scale climate modes provide useful predictive information and through which physical pathways that information reaches the coast.

The North Pacific Meridional Mode is a leading candidate source of seasonal predictability in the northeast Pacific. The NPMM reflects coupled atmosphere–ocean variability in the North Pacific subtropics and extratropics and is associated with wind–evaporation–SST feedbacks and subsequent Pacific climate evolution. If the NPMM projects onto coastal SST anomalies along the northeast Pacific boundary, then NPMM-conditioned analogs may improve prediction beyond forecasts based only on local or broad-domain SST persistence.

The Pacific Decadal Oscillation is another central mode of North Pacific SST variability. In this study we use the extratropical PDO component, here denoted PDOe. PDOe is expected to describe part of the background ocean state along the northeast Pacific margin. Unlike NPMM, which may provide a delayed forecast-period pathway through atmospheric and oceanic adjustment, PDOe may primarily represent a pre-existing coastal-ocean condition. Distinguishing these roles is essential because a predictor can be physically meaningful without being useful as a sole analog-selection variable. A background-state modifier can amplify or suppress another source of predictability even if it performs poorly when used alone.

A further question is whether the coastal SSTA response exhibits evidence of along-boundary propagation. Coastal trapped waves and coastal Kelvin waves can transmit sea-level and thermocline anomalies poleward along eastern boundaries. Their typical phase speeds are broadly of order 1–5 m s⁻¹. If NPMM-related coastal SSTA anomalies appear first in the southern part of the coastal path and later farther north, with inferred speeds compatible with this range, then the SST field may contain indirect evidence of a dynamically coherent coastal propagation pathway. However, SST alone cannot directly establish the presence of CTWs or coastal Kelvin waves. Direct confirmation would require SSH/SSHA, subsurface temperature, or other dynamical diagnostics.

This study uses an analog forecasting framework to test four questions:

1. Does adding NPMM information improve seasonal coastal SSTA prediction relative to a broad-domain SST-pattern baseline?
2. Does PDOe improve prediction as a sole analog predictor, or does it mainly modify the NPMM-related coastal response?
3. Is the coastal skill improvement ordered northward in a manner compatible with CTW/coastal Kelvin wave dynamics?
4. After removing NPMM–PDOe covariance, do both indices retain independent coastal SSTA signals, and what explains the asymmetry between warm and cold NPMM events?

The analysis emphasizes forecast leads 1–12 only. In this workflow, `LEAD0_PRIOR_MONTH=True`: lead 0 is the prior-month state and lead 1 is the first true forecast month. Lead 0 is therefore used only as an initial or background-state diagnostic and is not treated as forecast skill evidence.

---

## 2. Data and Methods

### 2.1 OISST observations

Observed SST is taken from OISST and used to define monthly SSTA over the northeast Pacific domain 30°–62°N, 195°–240°E. The observational analysis period is 1993–2019 for monthly OISST-based evaluation, with a 1994–2019 climatology base period used for anomaly construction. The preprocessing applies climatology removal and linear detrending. Daily OISST is also used for the submonthly ΔSSTA cross-lag propagation analysis.

### 2.2 CESM-HR analog library

The analog library is constructed from a high-resolution CESM-HR simulation spanning 1850–2014. The library provides a large sample of monthly climate states from which analogs are selected. CESM-HR SSTA is processed consistently with the observational target field, including climatology removal and linear detrending. The large library size allows repeated sampling of NPMM and PDOe phase states that are rare in the observational record.

### 2.3 NPMM and PDOe indices

The NPMM and PDOe indices are standardized as z-score time series. PDOe denotes the extratropical component of the Pacific Decadal Oscillation. Because NPMM and PDOe covary strongly, their raw phase composites cannot be interpreted as independent attribution tests. This covariance is explicitly addressed through residual analysis.

Monthly NPMM–PDOe covariance is large in both datasets:

| Dataset | Monthly Pearson r | Spearman ρ |
| ------- | ----------------: | ---------: |
| OISST   |             0.719 |      0.707 |
| CESM-HR |             0.558 |      0.539 |

The lead-lag maximum occurs at lag 0, indicating primarily simultaneous covariance rather than a simple lead–lag sequence.

### 2.4 Analog forecasting framework

For each observational initialization, analogs are selected from the CESM-HR library using combinations of broad-domain SST information and source indices. The principal experiments are:

| Experiment        | Analog-selection information            |
| ----------------- | --------------------------------------- |
| `wide_local_only` | Broad-domain SST pattern only; baseline |
| `wide_npmm`       | Broad-domain SST pattern + NPMM         |
| `wide_pdoe`       | Broad-domain SST pattern + PDOe         |
| `wide_npmm_pdoe`  | Broad-domain SST pattern + NPMM + PDOe  |

The forecast is evaluated at lead months 1–12. Lead 0 is excluded from forecast-period skill assessment because it represents the prior-month state.

### 2.5 Lead convention

The lead convention is central to the interpretation:

```
LEAD0_PRIOR_MONTH = True
lead 0 = prior-month state
lead 1 = first true forecast month
forecast-period results = lead 1–12 only
```

Therefore, lead 0 is not described as a forecast response, a forecast-period peak, or evidence of forecast skill. When lead 0 appears in diagnostics, it is interpreted only as a prior-month or initial/background-state diagnostic.

### 2.6 Coastal path and coastal bands

A continuous Amaya-style GSHHS coastal path is constructed over the domain from 30°N/240°E toward 62°N/195°E. The main path is a fjord-inclusive high-resolution coastline trace with total length 27,879 km. This geometry likely overestimates the dynamically relevant along-shelf distance. Consequently, speeds inferred from this path should be interpreted as lower bounds, whereas the monotonic ordering of arrival time along the path is more robust.

Coastal cells are grouped by offshore distance. Key coastal bands are ≤50 km, ≤100 km, and ≤150 km. The corresponding numbers of cells are:

| Coastal band | Number of cells |
| ------------ | --------------: |
| ≤50 km       |              87 |
| ≤100 km      |             127 |
| ≤150 km      |             168 |

Additional wider bands are used to test offshore decay of skill.

### 2.7 Skill metrics

The primary forecast-skill metric is source gain in ACC relative to the broad-domain SST baseline:

```
source_gain_acc = ACC(experiment) − ACC(wide_local_only)
```

The RMSE-based skill metric is:

```
source_gain_rmse_skill = 1 − RMSE(experiment) / RMSE(wide_local_only)
```

Spatial coverage is measured by `both_fraction`, the fraction of grid cells for which both ACC gain and RMSE skill are positive. A source is treated as practically credible only when ACC gain, RMSE skill, and `both_fraction` all indicate improvement.

### 2.8 Propagation analysis

Propagation is evaluated using three complementary SST-based diagnostics.

First, monthly skill Hovmöller diagrams are constructed from `source_gain_acc` as a function of along-coast distance and forecast lead. Along-coast bins of 500 km are used. Two arrival metrics are computed:

```
peak_lead = forecast lead, restricted to leads 1–12, at which source_gain_acc is maximum
arrival_centroid = Σ(lead × max(source_gain_acc, 0)) / Σ(max(source_gain_acc, 0))
```

Second, cell-level arrival-centroid estimates are evaluated along the coastal path. This provides an independent spatial estimate of arrival ordering and apparent speed.

Third, daily OISST ΔSSTA cross-lag analysis is used to reduce the persistence bias in raw NPMM–SSTA correlations. The preferred daily diagnostic is:

```
corr[NPMM_z(t), ΔSSTA(bin, t → t + lag)]
where ΔSSTA = SSTA(t + lag) − SSTA(t)
```

Raw `corr[NPMM(t), SSTA(t + lag)]` is not preferred because NPMM persistence inflates correlations across lags. The daily propagation estimate is truncated at 22,500 km, approximately Kodiak Island, because the full path includes a noisier Aleutian segment that weakens the fit.

Bootstrap and confidence-interval treatments follow the notebook workflow. The NB05 propagation CI uses 2,000 bootstrap resamples of 500 km bins. The NB09 daily ΔSSTA speed CI uses an OLS SE delta-method estimate.

### 2.9 Residual analysis for NPMM/PDOe independence

Because NPMM and PDOe covary strongly, residualized indices are used to test independent coastal SSTA signals:

```
NPMM_resid = NPMM_z − β × PDOe_z
PDOe_resid = PDOe_z − β × NPMM_z
```

Residual composites are evaluated using the lead 4–7 mean SSTA in the 150 km coastal band. Bootstrap 95% confidence intervals are used to assess statistical robustness.

---

## 3. Results

### 3.1 Baseline analog skill and domain-wide sanity check

The baseline `wide_local_only` analog forecast provides a skillful reference forecast before any source-index information is added. In the domain-wide sanity check over 30°–62°N, 195°–240°E, the baseline anomaly correlation coefficient decreases from 0.688 at lead 1 to 0.267 at lead 12. The baseline analog forecast exceeds persistence from lead 3 onward, indicating that the analog framework itself contains useful seasonal predictive information. Lead 0 is not included in this forecast-skill assessment because it represents the prior-month state rather than a forecast month.

| Lead | Baseline ACC | 95% CI         |
| ---: | -----------: | :------------- |
|    1 |        0.688 | [0.625, 0.742] |
|    3 |        0.470 | [0.379, 0.553] |
|    5 |        0.374 | [0.275, 0.466] |
|    7 |        0.328 | [0.225, 0.424] |
|    9 |        0.290 | [0.185, 0.388] |
|   12 |        0.267 | [0.160, 0.367] |

Adding NPMM information does not improve the domain-wide mean at the shortest leads. In the full-domain average, `wide_npmm` source_gain_acc is negative at lead 1, near-zero at lead 2, and positive from lead 3 onward. The domain-wide maximum occurs at lead 8, with `source_gain_acc = +0.0471`. This domain-wide timing is later than the coastal-band maximum, where the NPMM-related gain peaks at lead 6–7. The difference indicates that the predictable NPMM signal is sharper near the coastal boundary than in the full-domain mean.

| Lead | `wide_npmm` source_gain_acc | `wide_npmm_pdoe` source_gain_acc | `wide_pdoe` source_gain_acc |
| ---: | --------------------------: | -------------------------------: | --------------------------: |
|    1 |                     −0.0124 |                          −0.0104 |                     −0.0109 |
|    2 |                     −0.0005 |                          +0.0005 |                     −0.0063 |
|    3 |                     +0.0124 |                          +0.0080 |                     −0.0130 |
|    4 |                     +0.0085 |                          +0.0114 |                     −0.0146 |
|    5 |                     +0.0150 |                          +0.0169 |                     −0.0086 |
|    6 |                     +0.0311 |                          +0.0300 |                     −0.0027 |
|    7 |                     +0.0401 |                          +0.0361 |                     −0.0068 |
|    8 |                     +0.0471 |                          +0.0352 |                     −0.0056 |
|    9 |                     +0.0452 |                          +0.0350 |                     −0.0010 |
|   10 |                     +0.0336 |                          +0.0285 |                     −0.0114 |
|   11 |                     +0.0347 |                          +0.0202 |                     −0.0066 |
|   12 |                     +0.0307 |                          +0.0267 |                     −0.0068 |

### 3.2 Coastal skill improvement and lead-window dependence

The primary forecast-skill evidence comes from the coastal band rather than the domain-wide mean. In the ≤150 km coastal band, NPMM-conditioned analogs produce the strongest and most spatially coherent improvement at lead 6–7. For `wide_npmm`, lead 6–7 gives `source_gain_acc = +0.0542`, RMSE skill `= +0.0281`, and `both_fraction = 0.923`. For `wide_npmm_pdoe`, the same window gives `source_gain_acc = +0.0588`, RMSE skill `= +0.0317`, and `both_fraction = 0.961`. Thus, 92.3% of coastal cells improve in both ACC and RMSE when NPMM is added, and 96.1% improve when NPMM and PDOe are added together.

| Lead window | Experiment       | source_gain_acc | RMSE skill | both_fraction |
| ----------- | ---------------- | --------------: | ---------: | ------------: |
| lead1_3     | `wide_npmm`      |         +0.0184 |    +0.0105 |         0.687 |
| lead1_3     | `wide_npmm_pdoe` |         +0.0200 |    +0.0138 |         0.744 |
| lead1_3     | `wide_pdoe`      |         −0.0008 |    −0.0010 |         0.579 |
| lead4_5     | `wide_npmm`      |         +0.0417 |    +0.0226 |         0.848 |
| lead4_5     | `wide_npmm_pdoe` |         +0.0429 |    +0.0250 |         0.940 |
| lead4_5     | `wide_pdoe`      |         −0.0062 |    −0.0034 |         0.411 |
| lead6_7     | `wide_npmm`      |         +0.0542 |    +0.0281 |         0.923 |
| lead6_7     | `wide_npmm_pdoe` |         +0.0588 |    +0.0317 |         0.961 |
| lead6_7     | `wide_pdoe`      |         +0.0050 |    +0.0030 |         0.619 |
| lead8_12    | `wide_npmm`      |         +0.0298 |    +0.0111 |         0.695 |
| lead8_12    | `wide_npmm_pdoe` |         +0.0304 |    +0.0120 |         0.711 |
| lead8_12    | `wide_pdoe`      |         +0.0005 |          0 |         0.514 |

The lead dependence is physically informative. The NPMM contribution is weak at lead 1–3, strengthens at lead 4–5, peaks at lead 6–7, and then weakens at lead 8–12. This timing supports the interpretation that NPMM contributes a delayed forecast-period coastal pathway rather than only a contemporaneous SST-pattern correction. The combined `wide_npmm_pdoe` experiment exceeds `wide_npmm` in every lead window, but the incremental gain from adding PDOe is modest compared with the main NPMM-related improvement.

### 3.3 Offshore-distance dependence and PDOe-alone failure

The NPMM-related skill gain is concentrated near the coast and weakens offshore. For the lead 4–7 mean, `wide_npmm` source_gain_acc decreases from +0.0513 in the ≤50 km band to +0.0237 over the full usable domain. The `wide_npmm_pdoe` experiment shows the same offshore decay, decreasing from +0.0520 at ≤50 km to +0.0236 over the full usable domain. This distance dependence supports the interpretation that the predictable signal is coastal-boundary intensified rather than spatially uniform across the northeast Pacific domain.

|       Coastal band | n_cells | `wide_npmm` source_gain_acc | `wide_npmm` RMSE skill | `wide_npmm` both_fraction | `wide_npmm_pdoe` source_gain_acc | `wide_npmm_pdoe` both_fraction | `wide_pdoe` source_gain_acc | `wide_pdoe` both_fraction |
| -----------------: | ------: | --------------------------: | ---------------------: | ------------------------: | -------------------------------: | -----------------------------: | --------------------------: | ------------------------: |
|             ≤50 km |      87 |                     +0.0513 |                +0.0232 |                     0.871 |                          +0.0520 |                          0.922 |                     −0.0021 |                     0.520 |
|            ≤100 km |     127 |                     +0.0489 |                +0.0237 |                     0.872 |                          +0.0516 |                          0.937 |                     −0.0004 |                     0.537 |
|            ≤150 km |     168 |                     +0.0480 |                +0.0254 |                     0.885 |                          +0.0509 |                          0.951 |                     −0.0006 |                     0.515 |
|            ≤300 km |     270 |                     +0.0423 |                +0.0255 |                     0.869 |                          +0.0464 |                          0.955 |                     −0.0017 |                     0.462 |
|            ≤600 km |     459 |                     +0.0352 |                +0.0240 |                     0.835 |                          +0.0401 |                          0.922 |                     −0.0041 |                     0.391 |
| Full usable domain |   1,197 |                     +0.0237 |                +0.0143 |                     0.691 |                          +0.0236 |                          0.734 |                     −0.0081 |                     0.319 |

PDOe alone is generally unsuitable as a sole analog predictor. The `wide_pdoe` experiment is negative or near-neutral across most lead windows and spatial bands. Its clearest harmful effect occurs at lead 4–5 in the ≤150 km band, where `source_gain_acc = −0.0062`, RMSE skill `= −0.0034`, and `both_fraction = 0.411`. Across wider spatial bands, `wide_pdoe` both_fraction falls below 0.5 at ≥300 km, indicating that most cells are worse than the baseline when PDOe is used alone. PDOe is therefore better interpreted as a background-state modifier or as part of a joint NPMM–PDOe conditioning framework, not as a reliable standalone analog predictor.

### 3.4 SST-based coastal propagation evidence

The coastal propagation analysis provides strong indirect evidence for northward ordering of the NPMM-related coastal signal. It does not provide direct detection of a coastal trapped wave or coastal Kelvin wave. The evidence is based on SST-derived skill timing and daily ΔSSTA cross-lag structure; direct confirmation requires SSH/SSHA or subsurface temperature analysis.

Three independent diagnostics converge near 1.80–1.93 m s⁻¹. First, the NB05 monthly skill Hovmöller peak-lead OLS estimate for the 100 km band gives a speed of 1.80 m s⁻¹, with Spearman ρ = 0.814 and R² = 0.655. Second, the NB04 cell-level arrival_centroid OLS estimate for the 100 km band gives 1.929 m s⁻¹, with r = 0.810 and p = 8.2 × 10⁻³¹. Third, the NB09 daily ΔSSTA cross-lag analysis, truncated at 22,500 km, gives 1.87 m s⁻¹, with R² = 0.337, p = 6.3 × 10⁻⁶, and 95% CI [1.13, 2.61] m s⁻¹.

| Diagnostic                                         |   Band |       Speed | Supporting statistics                                 |
| -------------------------------------------------- | -----: | ----------: | ----------------------------------------------------- |
| NB05 monthly skill Hovmöller, peak-lead OLS        | 100 km |  1.80 m s⁻¹ | Spearman ρ = 0.814; R² = 0.655                        |
| NB04 cell-level arrival_centroid OLS               | 100 km | 1.929 m s⁻¹ | r = 0.810; p = 8.2 × 10⁻³¹                            |
| NB09 daily ΔSSTA cross-lag, truncated at 22,500 km | 100 km |  1.87 m s⁻¹ | R² = 0.337; p = 6.3 × 10⁻⁶; 95% CI [1.13, 2.61] m s⁻¹ |

The NB05 arrival_centroid OLS estimates are higher, approximately 2.88–3.09 m s⁻¹, and should be treated as complementary or upper-end estimates rather than as the only central propagation estimate. The NB05 bootstrap CI for the arrival_centroid metric gives 1.94 [1.49, 2.38] m s⁻¹ at the 100 km band. A conservative synthesis is that the northward ordering is robust and that the inferred SST-based propagation speed lies broadly within ~1.5–3.1 m s⁻¹, with three independent diagnostics clustering near 1.8–1.9 m s⁻¹. These values are compatible with the broad CTW/coastal Kelvin wave range of 1–5 m s⁻¹, but the present analysis remains indirect because it does not use SSH/SSHA or subsurface temperature.

The monthly phase-composite analysis does not resolve a robust propagation sequence, and this is expected. A signal traveling near 2 m s⁻¹ would traverse O(10³) km within roughly 8–14 days, so monthly averaging can smear submonthly along-coast timing and make the response appear nearly simultaneous over broad coastal segments. The daily ΔSSTA cross-lag diagnostic is therefore more appropriate for propagation inference. Raw `corr[NPMM(t), SSTA(t + lag)]` is biased by NPMM persistence, whereas `corr[NPMM(t), ΔSSTA(t → t + lag)]` reduces this persistence bias. The fjord-inclusive GSHHS path is also likely longer than the dynamically relevant along-shelf path, so the absolute speed estimates should be regarded as lower bounds; the monotonic along-coast ordering is the more robust result.

### 3.5 PDOe amplification of NPMM-related coastal SSTA

PDOe amplifies NPMM-related coastal SSTA when positive PDOe co-occurs with positive NPMM. The primary amplification metric is the lead 4–7 mean, evaluated during the forecast period. In CESM-HR, the lead 4–7 mean NPMM+ composite is +0.361°C, while the NPMM+/PDOe+ composite is +0.489°C. This corresponds to +35.4% amplification, with 95% CI [+24.8%, +47.7%]. In OISST, the lead 4–7 mean NPMM+ composite is +0.333°C, while NPMM+/PDOe+ is +0.471°C, corresponding to +41.5% amplification, with 95% CI [+16.3%, +73.7%].

| Dataset | NPMM+ lead 4–7 mean | NPMM+/PDOe+ lead 4–7 mean | Amplification |           95% CI |
| ------- | ------------------: | ------------------------: | ------------: | ---------------: |
| CESM-HR |            +0.361°C |                  +0.489°C |        +35.4% | [+24.8%, +47.7%] |
| OISST   |            +0.333°C |                  +0.471°C |        +41.5% | [+16.3%, +73.7%] |

Lead-1 amplification is useful only as an early forecast-period reference. At lead 1, CESM-HR NPMM+ coastal SSTA is +0.415°C, while NPMM+/PDOe+ is +0.685°C, giving +65.1% amplification. In OISST, the corresponding lead-1 amplification is +51.1%. The PDOe increment is +0.270°C at lead 1, +0.123°C at lead 6, and +0.119°C at lead 12. Thus, PDOe amplification is strongest early, weakens with lead time, and remains positive across forecast leads 1–12.

| Lead |    NPMM+ | NPMM+/PDOe+ | PDOe increment |
| ---: | -------: | ----------: | -------------: |
|    1 | +0.415°C |    +0.685°C |       +0.270°C |
|    6 | +0.345°C |    +0.469°C |       +0.123°C |
|   12 | +0.283°C |    +0.402°C |       +0.119°C |

These results support the interpretation that PDOe modifies the amplitude of the NPMM-related coastal response. They do not imply that PDOe alone is a reliable forecast predictor. Rather, PDOe conditions the coastal background state on which NPMM-related forcing acts.

### 3.6 NPMM/PDOe covariance and residual independence

NPMM and PDOe are strongly correlated. The monthly Pearson correlation is r = 0.719 in OISST and r = 0.558 in CESM-HR; the corresponding Spearman correlations are ρ = 0.707 and ρ = 0.539. The lead-lag maximum occurs at lag 0. Consequently, raw four-quadrant NPMM/PDOe phase composites are not independent attribution tests. Residual analysis is required to isolate the component of each index that is linearly independent of the other.

Residualized indices are defined as:

```
NPMM_resid = NPMM_z − β × PDOe_z
PDOe_resid = PDOe_z − β × NPMM_z
```

Using lead 4–7 mean SSTA in the 150 km coastal band, both residualized indices retain statistically significant coastal signals in both CESM-HR and OISST. All 95% bootstrap confidence intervals exclude zero. In CESM-HR, NPMM_resid+ gives +0.409°C, NPMM_resid− gives −0.685°C, PDOe_resid+ gives +0.473°C, and PDOe_resid− gives −0.189°C. In OISST, the corresponding values are +0.139°C, −0.327°C, +0.364°C, and −0.154°C.

| Dataset | Group       |   n | Mean SSTA | 95% bootstrap CI |
| ------- | ----------- | --: | --------: | ---------------: |
| CESM-HR | NPMM_resid+ | 120 |  +0.409°C | [+0.335, +0.477] |
| CESM-HR | NPMM_resid− | 120 |  −0.685°C | [−0.749, −0.623] |
| CESM-HR | PDOe_resid+ | 120 |  +0.473°C | [+0.411, +0.542] |
| CESM-HR | PDOe_resid− | 120 |  −0.189°C | [−0.251, −0.125] |
| OISST   | NPMM_resid+ |  60 |  +0.139°C | [+0.076, +0.200] |
| OISST   | NPMM_resid− |  60 |  −0.327°C | [−0.404, −0.247] |
| OISST   | PDOe_resid+ |  60 |  +0.364°C | [+0.282, +0.440] |
| OISST   | PDOe_resid− |  60 |  −0.154°C | [−0.217, −0.087] |

These residual composites show that both NPMM and PDOe retain independent coastal SSTA signals after removing shared variance. This is evidence for independent statistical contributions, not causal proof. The timing distinction is also important. NPMM_resid+ peaks at forecast lead 6, and NPMM_resid− peaks at forecast lead 3, indicating a delayed forecast-period coastal response at leads 3–6. PDOe-related SSTA is already present in the prior-month state, indicating that PDOe primarily encodes a pre-existing coastal ocean background condition. Forecast-period PDOe effects are therefore evaluated from lead 1 onward and through lead 4–7 mean amplification, not through lead 0.

### 3.7 Cooling asymmetry and its likely mechanism

The residual composites reveal a robust cooling asymmetry. In CESM-HR, NPMM_resid− cooling has magnitude 0.685°C, while NPMM_resid+ warming is +0.409°C, giving a ratio of 1.68×. In OISST, the corresponding values are −0.327°C and +0.139°C, giving a ratio of 2.35×. The asymmetry is statistically robust because the warm and cold residualized composite confidence intervals do not overlap in either dataset.

| Dataset | \|NPMM_resid−\| | NPMM_resid+ | Ratio |
| ------- | ------------: | ----------: | ----: |
| CESM-HR |       0.685°C |    +0.409°C | 1.68× |
| OISST   |       0.327°C |    +0.139°C | 2.35× |

Three mechanisms were evaluated. First, NPMM− events have slightly larger absolute z-score magnitudes than NPMM+ events. The cold/warm |z| ratio is 1.25× in OISST and 1.17× in CESM-HR. This contributes to the asymmetry but is too small to explain the full SSTA ratio.

Second, cold-PDOe co-occurrence is the most likely dominant contributor. In observations, 80.0% of NPMM− events co-occur with PDOe−, and the mean PDOe given NPMM− is −0.943. Thus, cold NPMM forcing often occurs on a cold PDOe background. The resulting asymmetry is consistent with nonlinear amplification when cold NPMM forcing acts on a cold PDOe background. This wording is intentionally conservative: the composites support a physically plausible interaction, but they do not by themselves prove a causal mechanism.

Third, seasonal sampling does not explain the asymmetry. NPMM+ and NPMM− events are distributed approximately evenly across DJF, MAM, JJA, and SON, with seasonal fractions near 23–26%. The cooling asymmetry is therefore unlikely to be a seasonal-composition artifact.

### 3.8 Case-level regression caution

Case-level regressions provide only weak support for source-index dependence of individual forecast improvements. Most regressions of case-level MSE improvement onto NPMM_z, PDOe_z, and their interaction are weak or statistically insignificant. The only notable result is for the `lead4_7 × wide_npmm_pdoe` configuration, where `r_npmm = −0.096`, `p = 0.001`, and `n = 1,248`. The negative sign suggests that stronger NPMM values are associated with smaller case-level improvement in this combined-source experiment. This result should be treated as a secondary cautionary finding, potentially reflecting overfitting, sampling noise, or imperfect analog matching under strong-source conditions. It is not a primary result and does not supersede the coastal-band skill, propagation, amplification, and residual-composite evidence.

---

## 4. Discussion

### 4.1 Physical interpretation: distinct NPMM and PDOe roles

The results support a two-component interpretation of northeast Pacific coastal SSTA predictability. The first component is a delayed NPMM-related forecast-period pathway. NPMM-conditioned analogs produce their strongest coastal forecast improvement at lead 6–7, when `wide_npmm` reaches `source_gain_acc = +0.0542`, RMSE skill `= +0.0281`, and `both_fraction = 0.923` in the ≤150 km coastal band. The combined `wide_npmm_pdoe` experiment reaches `source_gain_acc = +0.0588`, RMSE skill `= +0.0317`, and `both_fraction = 0.961` over the same lead window. This peak occurs earlier and more sharply in the coastal band than in the domain-wide mean, where NPMM source gain peaks at lead 9. The contrast indicates that NPMM-related predictability is concentrated near the coastal boundary and is partly obscured when averaged over the full northeast Pacific domain.

Residual composites reinforce this timing interpretation. After removing shared NPMM–PDOe variance, NPMM_resid+ peaks at forecast lead 6, while NPMM_resid− peaks at forecast lead 3. Thus, the NPMM signal is best interpreted as a delayed forecast-period coastal response at leads 3–6, with its clearest forecast-skill expression at lead 6–7. This timing is consistent with a pathway involving large-scale atmospheric forcing, air–sea interaction, and subsequent coastal-ocean adjustment, but it does not by itself establish a unique dynamical mechanism.

The second component is a PDOe-related background-state modifier. PDOe alone is not generally useful as a sole analog predictor. In the ≤150 km coastal band at lead 4–5, `wide_pdoe` has `source_gain_acc = −0.0062`, RMSE skill `= −0.0034`, and `both_fraction = 0.411`. In the lead 4–7 coastal-band analysis, `wide_pdoe` remains negative or near-neutral, and its `both_fraction` falls below 0.5 at ≥300 km. These results indicate that PDOe-based analog selection by itself is unstable or harmful relative to the broad-domain SST-pattern baseline.

PDOe is nevertheless physically relevant when interpreted as a modifier of the NPMM-related coastal response. The primary forecast-period amplification metric is the lead 4–7 mean: NPMM+/PDOe+ amplifies NPMM+ coastal SSTA by +35.4% in CESM-HR, with 95% CI [+24.8%, +47.7%], and by +41.5% in OISST, with 95% CI [+16.3%, +73.7%]. Lead-1 amplification is also large, +65.1% in CESM-HR and +51.1% in OISST, but should be interpreted only as an early forecast-period reference. Forecast-period PDOe influence should therefore be evaluated from lead 1 onward, especially through the lead 4–7 mean amplification.

PDOe-related SSTA is already present in the prior-month state, indicating that PDOe primarily encodes a pre-existing coastal ocean background condition. This is a different role from the delayed NPMM pathway. The useful distinction is therefore not "NPMM active, PDOe passive," but rather "NPMM as a delayed forecast-period pathway" and "PDOe as a pre-existing background-state modifier." This distinction also explains why PDOe can have statistically significant residual composite signals while still performing poorly as a standalone analog-selection variable.

### 4.2 Propagation evidence: consistent with CTW/coastal Kelvin dynamics, but indirect

The propagation diagnostics provide strong indirect evidence for northward ordering of the NPMM-related coastal signal. Three independent SST-based estimates converge near 1.80–1.93 m s⁻¹: NB05 peak-lead OLS gives 1.80 m s⁻¹ for the 100 km band, with Spearman ρ = 0.814 and R² = 0.655; NB04 cell-level arrival_centroid OLS gives 1.929 m s⁻¹, with r = 0.810 and p = 8.2 × 10⁻³¹; and NB09 daily ΔSSTA cross-lag gives 1.87 m s⁻¹, with 95% CI [1.13, 2.61] m s⁻¹, R² = 0.337, and p = 6.3 × 10⁻⁶ after truncation at 22,500 km. The NB05 arrival_centroid OLS estimate is higher, approximately 2.88–3.09 m s⁻¹, and is best treated as a complementary or upper-end estimate rather than the only central value.

The conservative synthesis is that the northward ordering is robust and that the inferred SST-based propagation speed lies broadly within ~1.5–3.1 m s⁻¹, with three independent diagnostics clustering near 1.8–1.9 m s⁻¹. These speeds are compatible with the broad CTW/coastal Kelvin wave range of 1–5 m s⁻¹. However, compatibility is not direct detection. The present evidence is based on SST, skill ordering, and ΔSSTA timing, not on direct dynamical variables. Direct confirmation requires SSH/SSHA or subsurface temperature analysis.

Several guardrails are essential. First, a skill Hovmöller diagonal is evidence of ordered forecast improvement, not proof of physical wave propagation. Skill can improve along a coast because of a propagating oceanic signal, but it can also reflect spatially organized atmospheric forcing or source-index-dependent persistence. Second, the fjord-inclusive GSHHS path has a total length of 27,879 km and likely overestimates the dynamically relevant along-shelf distance. Therefore, absolute speed estimates derived from this path are likely lower bounds. The ordering itself, especially the rank-based Spearman structure, is more robust than the absolute speed.

Monthly phase composites cannot resolve the inferred propagation. A signal traveling at approximately 2 m s⁻¹ would traverse the relevant coastal domain in roughly 8–14 days. Monthly averaging would therefore make the response appear nearly simultaneous along the coast. This explains why monthly composite propagation diagnostics do not yield a clear along-coast sequence and why daily diagnostics are required. The absence of a monthly resolved sequence should not be interpreted as evidence against fast coastal propagation.

The daily ΔSSTA cross-lag diagnostic is preferred over raw lagged SSTA correlation because raw `corr[NPMM(t), SSTA(t + lag)]` is inflated by NPMM persistence. By using `corr[NPMM(t), ΔSSTA(t → t + lag)]`, the analysis focuses on the change in SSTA after the initial state and reduces persistence bias. This does not eliminate all ambiguity, but it is a more defensible SST-based propagation diagnostic than raw lagged correlation.

### 4.3 Implications for analog prediction

The forecast results imply that source information should be used selectively. NPMM provides the main useful additional predictor for coastal SSTA, especially within 50–150 km of the coast and at lead 6–7. The offshore decay of skill indicates that evaluating only a full-domain mean can understate coastal predictability. For applications focused on Alaska or northeast Pacific coastal SST, coastal-band skill should be treated as the primary evaluation target.

PDOe should not be used as a sole analog predictor in this framework. Its standalone analog performance is negative or near-neutral across most lead windows and bands, and it is clearly harmful at lead 4–5 in the ≤150 km band. However, PDOe remains useful as a conditioning variable. It modifies the amplitude and sign of the NPMM-related coastal response and improves spatial coverage when combined with NPMM in `wide_npmm_pdoe`. This suggests that analog systems may benefit from treating PDOe as a stratification or modifier variable rather than as an independent nearest-neighbor matching source.

The combined `wide_npmm_pdoe` experiment consistently exceeds `wide_npmm` in the principal coastal lead windows, but the improvement from adding PDOe is modest relative to the NPMM gain itself. This distinction matters operationally. A forecast system that overweights PDOe may degrade analog matching, while a system that ignores PDOe may miss important amplitude modulation. A conservative implementation would use NPMM for primary analog selection and PDOe for phase-aware conditioning or post-selection interpretation.

### 4.4 NPMM–PDOe covariance and residual interpretation

NPMM and PDOe are strongly correlated, with monthly Pearson r = 0.719 in OISST and r = 0.558 in CESM-HR. Their lead-lag maximum occurs at lag 0. Therefore, raw four-quadrant composites are not independent attribution tests. A raw NPMM+ composite includes part of the PDOe background, and a raw PDOe+ composite includes part of the NPMM signal.

Residual analysis addresses this covariance by removing the shared linear component. At lead 4–7 in the 150 km coastal band, all residual composite confidence intervals exclude zero. In CESM-HR, NPMM_resid+ is +0.409°C with 95% CI [+0.335, +0.477], NPMM_resid− is −0.685°C with 95% CI [−0.749, −0.623], PDOe_resid+ is +0.473°C with 95% CI [+0.411, +0.542], and PDOe_resid− is −0.189°C with 95% CI [−0.251, −0.125]. In OISST, the corresponding values are +0.139°C, −0.327°C, +0.364°C, and −0.154°C, again with confidence intervals excluding zero.

These residual composites support two conclusions. First, NPMM and PDOe each retain statistically significant coastal SSTA signals after removing shared variance. Second, residual significance does not prove causality. The residual method reduces linear covariance but cannot rule out nonlinear confounding, shared atmospheric drivers, or model-dependent event sampling. The appropriate interpretation is that NPMM and PDOe contain separable statistical information about coastal SSTA, with different timing characteristics and different forecast roles.

### 4.5 Cooling asymmetry as PDOe-modulated nonlinear amplification

The residual composites show a robust cooling asymmetry. In CESM-HR, NPMM_resid− cooling is −0.685°C, while NPMM_resid+ warming is +0.409°C, giving a magnitude ratio of 1.68×. In OISST, the corresponding values are −0.327°C and +0.139°C, giving a ratio of 2.35×. The warm and cold residualized confidence intervals do not overlap in either dataset, so the asymmetry is unlikely to be a sampling artifact.

The asymmetry has more than one contributor. NPMM− events have somewhat larger absolute standardized amplitudes than NPMM+ events: the cold/warm |z| ratio is 1.25× in OISST and 1.17× in CESM-HR. This can explain part of the asymmetry but not the full SSTA ratio. Seasonal sampling is also unlikely to be the main cause because NPMM+ and NPMM− events are distributed approximately evenly across DJF, MAM, JJA, and SON.

The most plausible dominant contributor is preferential co-occurrence between cold NPMM and cold PDOe. In observations, 80.0% of NPMM− events co-occur with PDOe−, and the mean PDOe conditional on NPMM− is −0.943. Thus, cold NPMM forcing frequently acts on an already cold PDOe background. The resulting cooling asymmetry is consistent with nonlinear amplification when cold NPMM forcing acts on a cold PDOe background. This statement should remain an inference rather than a causal claim. The evidence supports PDOe-modulated amplification, but it does not isolate the full dynamical mechanism responsible for the amplification.

This asymmetry has forecast relevance. Cold-event forecasts may require explicit phase-aware interpretation because NPMM− events preferentially co-occur with PDOe−; this should be treated as a composite-level inference rather than case-level proof of higher predictability. This suggests that phase-aware forecast interpretation may be particularly important for cold-event risk assessment.

### 4.6 Limitations and future work

Several limitations constrain the interpretation.

First, the along-coast distance metric is path-dependent. The main GSHHS path is fjord-inclusive and therefore likely overestimates the dynamically relevant along-shelf distance. This makes propagation speeds lower bounds. Future work should repeat the analysis using an outer-shelf or shelf-break path while preserving a consistent distance convention. Rank ordering can be compared across paths, but OLS speeds cannot be directly compared when path lengths differ.

Second, the propagation evidence is SST-only and indirect. SST contains the surface expression of multiple processes, including air–sea heat fluxes, mixed-layer entrainment, alongshore advection, and remote oceanic wave adjustment. The current analysis is compatible with CTW/coastal Kelvin wave dynamics, but SSH/SSHA or subsurface temperature is required for direct confirmation. A stronger test would examine poleward propagation in sea level, thermocline depth, subsurface temperature, or dynamically consistent coastal pressure gradients.

Third, the observational record is short. OISST provides only a few decades of monthly data for phase composites and forecast verification. CESM-HR supplies a much larger event library, but model event amplitudes and NPMM–PDOe co-occurrence statistics may differ from observations. The stronger observed cooling asymmetry, for example, may partly reflect the specific 1993–2019 sampling of Pacific background states.

Fourth, case-level regression provides weak support for individual-event predictability. Most regressions are insignificant, and the only notable result is `lead4_7 × wide_npmm_pdoe`, with `r_npmm = −0.096`, `p = 0.001`, and `n = 1,248`. This weak negative relationship may reflect overfitting, noisy case-level variability, or imperfect analog matching under strong-source conditions. The main evidence for predictability is therefore composite-level and spatially aggregated, not case-by-case linear dependence.

Fifth, PDOe-alone analog selection is harmful or unstable in this framework. This limitation is also a practical warning: a statistically meaningful climate mode is not automatically a useful analog-selection variable. PDOe should be handled as a background-state modifier or conditioning factor unless further optimization shows that it can be used without degrading baseline analog matching.

---

## 5. Conclusions

This study used an analog forecasting framework based on a CESM-HR library and OISST observations to evaluate the roles of NPMM and PDOe in seasonal prediction of coastal SSTA along the northeast Pacific and Alaska coast. The main conclusions are as follows.

1. **The analog framework is skillful, and NPMM adds its clearest value in the coastal band.** The baseline `wide_local_only` forecast has domain-wide ACC of 0.688 at lead 1 and 0.267 at lead 12 and exceeds persistence from lead 3 onward. NPMM source gain is negative or near-zero in the domain-wide mean at leads 1–2 but becomes positive from lead 3 onward. The coastal-band response is sharper: in the ≤150 km band, the strongest NPMM-related improvement occurs at lead 6–7.

2. **Lead 6–7 is the central coastal skill-improvement window.** In the ≤150 km coastal band, `wide_npmm` reaches `source_gain_acc = +0.0542`, RMSE skill `= +0.0281`, and `both_fraction = 0.923` at lead 6–7. The combined `wide_npmm_pdoe` experiment reaches `source_gain_acc = +0.0588`, RMSE skill `= +0.0317`, and `both_fraction = 0.961`. Skill decreases offshore, supporting the interpretation that the predictable NPMM-related signal is concentrated near the coastal boundary.

3. **PDOe alone is generally unsuitable as a sole analog predictor.** The `wide_pdoe` experiment is negative or near-neutral across most lead windows and coastal bands. At lead 4–5 in the ≤150 km coastal band, it has `source_gain_acc = −0.0062` and `both_fraction = 0.411`. Across wider bands, its `both_fraction` falls below 0.5 at ≥300 km. PDOe is therefore useful primarily as a modifier of NPMM-related predictability, not as an independent analog-selection source.

4. **PDOe acts as a pre-existing background-state modifier, whereas NPMM produces a delayed forecast-period response.** NPMM_resid+ peaks at forecast lead 6, and NPMM_resid− peaks at forecast lead 3, consistent with a delayed coastal response at leads 3–6. PDOe-related SSTA is already present in the prior-month state, indicating that PDOe primarily encodes a pre-existing coastal ocean background condition. Forecast-period PDOe influence should be evaluated from lead 1 onward, especially through the lead 4–7 mean amplification.

5. **The primary PDOe amplification metric is the lead 4–7 mean.** In CESM-HR, NPMM+/PDOe+ amplifies NPMM+ coastal SSTA by +35.4%, with 95% CI [+24.8%, +47.7%]. In OISST, the corresponding amplification is +41.5%, with 95% CI [+16.3%, +73.7%]. Lead-1 amplification is larger, +65.1% in CESM-HR and +51.1% in OISST, but it is best treated as an early forecast-period reference rather than the primary metric.

6. **SST-based propagation evidence is consistent with CTW/coastal Kelvin wave dynamics but remains indirect.** Three diagnostics converge near 1.80–1.93 m s⁻¹: NB05 peak-lead OLS gives 1.80 m s⁻¹, NB04 cell-level arrival_centroid gives 1.929 m s⁻¹, and NB09 daily ΔSSTA cross-lag gives 1.87 [1.13, 2.61] m s⁻¹. These values are compatible with broad CTW/coastal Kelvin wave speeds. However, the evidence is based on SST and skill-ordering diagnostics, not direct dynamical wave variables. SSH/SSHA or subsurface temperature data are required for direct confirmation.

7. **NPMM and PDOe retain independent coastal SSTA signals after removing shared variance.** NPMM and PDOe are strongly correlated, with monthly Pearson r = 0.719 in OISST and r = 0.558 in CESM-HR, so raw phase composites are not independent attribution tests. Residual composites at lead 4–7 in the 150 km coastal band show statistically significant signals for both indices. In CESM-HR, NPMM_resid+ is +0.409°C, NPMM_resid− is −0.685°C, PDOe_resid+ is +0.473°C, and PDOe_resid− is −0.189°C. In OISST, the corresponding values are +0.139°C, −0.327°C, +0.364°C, and −0.154°C. All 95% bootstrap confidence intervals exclude zero.

8. **The cooling asymmetry is robust and is consistent with PDOe-modulated amplification.** NPMM_resid− cooling is 1.68× stronger than NPMM_resid+ warming in CESM-HR and 2.35× stronger in OISST. The asymmetry is only partly explained by larger NPMM− event amplitude, with cold/warm |z| ratios of 1.17× in CESM-HR and 1.25× in OISST. The more likely dominant contributor is cold-PDOe co-occurrence: 80.0% of observed NPMM− events co-occur with PDOe−, and mean PDOe given NPMM− is −0.943. This supports a conservative interpretation in which cold NPMM forcing is amplified when it acts on a cold PDOe background, without implying causal proof beyond the residual-composite evidence.

9. **Case-level regression is secondary and cautionary.** Most case-level regressions are weak or insignificant. The only notable result is `lead4_7 × wide_npmm_pdoe`, with `r_npmm = −0.096`, `p = 0.001`, and `n = 1,248`. This weak negative association may reflect overfitting, case-level noise, or imperfect analog matching under strong-source conditions. It should not be treated as a primary result.

---

## 6. Claims Requiring Caution or Further Verification

1. **CTW/coastal Kelvin wave interpretation** — The propagation speeds are compatible with CTW/coastal Kelvin wave dynamics, but this is not direct detection. The current evidence is SST-based and indirect. SSH/SSHA or subsurface temperature analysis is required for direct confirmation.

2. **Absolute propagation speed** — The GSHHS coastal path is fjord-inclusive and likely overestimates true along-shelf distance. Therefore, the reported speed estimates should be treated as lower bounds. The along-coast ordering is more robust than the absolute speed.

3. **Monthly phase composites** — Monthly composites cannot resolve a ~2 m s⁻¹ coastal signal because such a signal would cross the domain in roughly 8–14 days. A lack of monthly propagation structure does not contradict the daily ΔSSTA result.

4. **PDOe attribution** — PDOe residual composites show independent coastal SSTA signals after removing shared NPMM variance, but this does not prove a causal mechanism. PDOe should be described as a background-state modifier, not as a standalone forecast pathway.

5. **Case-level regression** — Most case-level regressions are weak or insignificant. The notable `lead4_7 × wide_npmm_pdoe` result, `r_npmm = −0.096`, `p = 0.001`, `n = 1,248`, should be treated as a cautionary indication of possible overfitting or case-level noise.

6. **Observational sample size** — OISST composites are limited by the short observational record. CESM-HR provides larger samples but may not perfectly reproduce observed event amplitude or NPMM–PDOe co-occurrence statistics.

---

## 7. Figure Plan

### Figure 1. Study domain, coastal path, and coastal bands

Map of the 30°–62°N, 195°–240°E domain showing the GSHHS fjord-inclusive coastal path, the 50 km, 100 km, and 150 km coastal bands, and the 22,500 km truncation point near Kodiak Island. Caption should note that the path length is 27,879 km and that fjord inclusion likely makes speed estimates lower bounds.

**Panel A — Full GSHHS coastal path with Alaska-aware routing:**

![Full GSHHS coastal path](figures/nb09_daily_sst_propagation_30_62/00_alaska_aware_gshhs_coast_path.png)

**Panel B — Coastal band map (≤150 km band shown):**

![Coastal band map le150km](figures/nb04_coastal_path_30_62/le150km/path_map_le150km.png)

**Panel C — Three-path sensitivity comparison (GSHHS / outer coast / shelf break):**

![Three paths comparison map](figures/nb04_coastal_path_30_62/three_paths_comparison_map.png)

---

### Figure 2. Baseline skill and domain-wide source gain

Lead-dependent ACC for the `wide_local_only` baseline and source_gain_acc for `wide_npmm`, `wide_npmm_pdoe`, and `wide_pdoe` in the domain-wide sanity check. The figure should emphasize that baseline ACC declines from 0.688 at lead 1 to 0.267 at lead 12, while domain-wide NPMM source gain becomes positive from lead 3 onward and peaks at lead 8.

*Source: `figures/nb03_grid_skill_maps_30_62/alaska_grid1440_30_62/alaska_grid1440_30_62_source_gain_vs_wide_local.csv` — domain-wide cell-level temporal ACC, averaged across all grid cells (lead 1–12 only).*

| Lead | Baseline ACC | Persistence ACC | `wide_npmm` gain | `wide_npmm_pdoe` gain | `wide_pdoe` gain |
| ---: | -----------: | --------------: | ---------------: | --------------------: | ---------------: |
|    1 |        0.688 |           0.754 |          −0.0124 |               −0.0104 |          −0.0109 |
|    2 |        0.558 |           0.557 |          −0.0005 |               +0.0005 |          −0.0063 |
|    3 |        0.470 |           0.440 |          +0.0124 |               +0.0080 |          −0.0130 |
|    4 |        0.410 |           0.381 |          +0.0085 |               +0.0114 |          −0.0146 |
|    5 |        0.374 |           0.341 |          +0.0150 |               +0.0169 |          −0.0086 |
|    6 |        0.350 |           0.316 |          +0.0311 |               +0.0300 |          −0.0027 |
|    7 |        0.328 |           0.308 |          +0.0401 |               +0.0361 |          −0.0068 |
|    8 |        0.301 |           0.294 |          +0.0471 |               +0.0352 |          −0.0056 |
|    9 |        0.290 |           0.279 |          +0.0452 |               +0.0350 |          −0.0010 |
|   10 |        0.289 |           0.254 |          +0.0336 |               +0.0285 |          −0.0114 |
|   11 |        0.274 |           0.230 |          +0.0347 |               +0.0202 |          −0.0066 |
|   12 |        0.267 |           0.212 |          +0.0307 |               +0.0267 |          −0.0068 |

> **Note:** All values are per-cell temporal ACC averaged across 1,197 usable domain cells (NB03; Fisher Z 95% CI available). A line plot figure is not yet generated.

---

### Figure 3. Coastal lead-window skill summary

Bar or line plot of coastal ≤150 km source_gain_acc, RMSE skill, and both_fraction for lead1_3, lead4_5, lead6_7, and lead8_12. The lead 6–7 peak should be highlighted: `wide_npmm = +0.0542`, `wide_npmm_pdoe = +0.0588`.

![Lead window source gain — coastal path ≤150 km](figures/nb07_lead_windows_30_62/lead_window_source_gain_coastalpath_le150km.png)

---

### Figure 4. Offshore-distance dependence of skill

Coastal-band plot showing source_gain_acc and both_fraction as a function of offshore band width for `wide_npmm`, `wide_npmm_pdoe`, and `wide_pdoe` at lead 4–7. The figure should show monotonic offshore weakening for NPMM-based skill and the poor performance of PDOe alone, including both_fraction below 0.5 at ≥300 km.

![Source gain by coastal distance — lead 4–7](figures/nb06_coastal_band_30_62/lead4_7_source_gain_by_coastal_distance.png)

---

### Figure 5. Monthly skill Hovmöller and arrival diagnostics

Hovmöller diagram of source_gain_acc as a function of along-coast distance and lead for `wide_npmm`, with peak_lead and arrival_centroid markers. Include the 100 km band OLS result: peak-lead speed 1.80 m s⁻¹, Spearman ρ = 0.814, R² = 0.655. The caption should state that this is skill-ordering evidence, not direct wave detection.

**Panel A — Skill Hovmöller (wide_npmm, ≤100 km band):**

![Skill Hovmöller wide_npmm le100km](figures/nb05_skill_propagation_maps_30_62/02_skill_hovmoller/skill_hov_wide_npmm_le100km.png)

**Panel B — Peak-lead propagation scatter (wide_npmm, ≤100 km band):**

![Peak-lead scatter wide_npmm le100km](figures/nb05_skill_propagation_maps_30_62/03_skill_propagation_scatter/peak_lead/scatter_wide_npmm_le100km.png)

**Panel C — Arrival-centroid propagation scatter (wide_npmm, ≤100 km band):**

![Arrival-centroid scatter wide_npmm le100km](figures/nb05_skill_propagation_maps_30_62/03_skill_propagation_scatter/arrival_centroid/scatter_wide_npmm_le100km.png)

**Panel D — Bootstrap speed CI (arrival_centroid):**

![Bootstrap CI arrival centroid](figures/nb05_skill_propagation_maps_30_62/speed_bootstrap_ci_arrival_centroid.png)

**Panel E — Bootstrap speed CI (bar summary):**

![Bootstrap CI bar](figures/nb05_skill_propagation_maps_30_62/speed_bootstrap_ci_bar.png)

---

### Figure 6. Daily ΔSSTA cross-lag propagation estimate

Plot of peak ΔSSTA cross-lag timing versus along-coast distance for the 100 km band truncated at 22,500 km. Include OLS speed 1.87 m s⁻¹, R² = 0.337, p = 6.3 × 10⁻⁶, and 95% CI [1.13, 2.61] m s⁻¹. Caption should explain why ΔSSTA is preferred over raw lagged SSTA correlation.

**Panel A — ΔSSTA cross-lag Hovmöller (≤100 km band):**

![Cross-lag delta Hovmöller le100km](figures/nb09_daily_sst_propagation_30_62/06_crosslag_delta/crosslag_delta_hov_npmm_le100km.png)

**Panel B — ΔSSTA cross-lag propagation scatter, truncated at ~23,000 km (≤100 km band):**

![Cross-lag delta scatter le100km trunc23000](figures/nb09_daily_sst_propagation_30_62/06_crosslag_delta/trunc_23000km/scatter_delta_le100km_trunc23000.png)

**Panel C — OLS SE confidence interval for daily propagation speed:**

![Delta cross-lag speed OLS SE CI](figures/nb09_daily_sst_propagation_30_62/delta_crosslag_speed_ols_se_ci.png)

**Panel D — Bootstrap CI for daily propagation speed:**

![Delta cross-lag speed bootstrap CI](figures/nb09_daily_sst_propagation_30_62/delta_crosslag_speed_bootstrap_ci.png)

---

### Figure 7. PDOe amplification of NPMM+ coastal SSTA

Lead-dependent coastal-band SSTA composites for NPMM+ and NPMM+/PDOe+ in CESM-HR and OISST. Mark lead 1 amplification and lead 4–7 mean amplification. Include CESM-HR +65.1% at lead 1 and +35.4% [+24.8%, +47.7%] at lead 4–7; include OISST +51.1% at lead 1 and +41.5% [+16.3%, +73.7%] at lead 4–7.

**Panel A — PDOe increment line plot (CESM-HR library):**

![PDOe increment lib](figures/nb10_phase_propagation_monthly_30_62/07_pdoe_increment_line/pdoe_increment_lib.png)

**Panel B — PDOe increment line plot (OISST observations):**

![PDOe increment obs](figures/nb10_phase_propagation_monthly_30_62/07_pdoe_increment_line/pdoe_increment_obs.png)

**Panel C — Bootstrap CI composite (CESM-HR, ≤100 km band):**

![Composite CI lib le100km](figures/nb10_phase_propagation_monthly_30_62/08_bootstrap_ci/composite_ci_lib_le100km.png)

**Panel D — Bootstrap CI composite (OISST, ≤100 km band):**

![Composite CI obs le100km](figures/nb10_phase_propagation_monthly_30_62/08_bootstrap_ci/composite_ci_obs_le100km.png)

**Panel E — Band-average composite by phase (CESM-HR library):**

![Band avg composite lib](figures/nb10_phase_propagation_monthly_30_62/05_band_avg_composite/band_avg_composite_lib.png)

**Panel F — Band-average composite by phase (OISST observations):**

![Band avg composite obs](figures/nb10_phase_propagation_monthly_30_62/05_band_avg_composite/band_avg_composite_obs.png)

---

### Figure 8. Residual composites and cooling asymmetry

Lead-dependent residual composites for NPMM_resid± and PDOe_resid±, with lead 4–7 mean confidence intervals. Highlight NPMM_resid+ = +0.409°C and NPMM_resid− = −0.685°C in CESM-HR, and the corresponding 1.68× cooling asymmetry. Include OISST residual composites as observational verification.

**Panel A — Residual composite CI bar (CESM-HR + OISST combined):**

![Residual composite CI bar](figures/nb11_source_modifier_tests_30_62/E_residual_composite_ci/residual_composite_ci_bar.png)

**Panel B — Cooling asymmetry (CESM-HR library):**

![Cooling asymmetry CESM-HR](figures/nb11_source_modifier_tests_30_62/D_cooling_asymmetry/D_cooling_asymmetry_cesm-hr_lib.png)

**Panel C — Cooling asymmetry (OISST observations):**

![Cooling asymmetry OISST](figures/nb11_source_modifier_tests_30_62/D_cooling_asymmetry/D_cooling_asymmetry_oisst_obs.png)

**Panel D — NPMM_resid+ Hovmöller (CESM-HR library):**

![NPMM resid+ Hovmöller lib](figures/nb11_source_modifier_tests_30_62/B_residual_hovmoller/hov_lib__NPMM_resid+.png)

**Panel E — NPMM_resid− Hovmöller (CESM-HR library):**

![NPMM resid- Hovmöller lib](figures/nb11_source_modifier_tests_30_62/B_residual_hovmoller/hov_lib__NPMM_resid-.png)

**Panel F — NPMM_resid+ Hovmöller (OISST observations):**

![NPMM resid+ Hovmöller obs](figures/nb11_source_modifier_tests_30_62/B_residual_hovmoller/hov_obs__NPMM_resid+.png)

**Panel G — NPMM_resid− Hovmöller (OISST observations):**

![NPMM resid- Hovmöller obs](figures/nb11_source_modifier_tests_30_62/B_residual_hovmoller/hov_obs__NPMM_resid-.png)

---

## 8. Table Plan

### Table 1. Data sources, preprocessing, and lead convention

Include OISST period, CESM-HR library period, domain, climatology base period, detrending, lead convention, coastal path length, and coastal-band cell counts.

### Table 2. Domain-wide baseline and source-gain sanity check

Include baseline ACC at leads 1, 3, 5, 7, 9, and 12; include domain-wide `wide_npmm`, `wide_npmm_pdoe`, and `wide_pdoe` source_gain_acc by lead or selected leads.

### Table 3. Coastal skill by lead window and experiment

Include ≤150 km coastal-band lead1_3, lead4_5, lead6_7, and lead8_12 values for source_gain_acc, RMSE skill, and both_fraction for `wide_npmm`, `wide_npmm_pdoe`, and `wide_pdoe`.

### Table 4. Propagation speed estimates

Include NB05 peak-lead OLS, NB05 arrival-centroid OLS, NB05 bootstrap CI, NB04 cell-level arrival_centroid OLS, and NB09 daily ΔSSTA cross-lag speed with confidence intervals and statistics.

### Table 5. Residual composite and amplification statistics

Include NPMM/PDOe covariance, lead 4–7 residual composite means and 95% bootstrap CIs, PDOe amplification at lead 1 and lead 4–7, and cooling asymmetry ratios.

---

## References

*Placeholders only; complete bibliographic details to be verified before submission.*

Amaya et al. — NPMM-related analog forecasting and coastal SST predictability.

Bond et al. — Northeast Pacific marine heatwave and ecosystem impacts.

Cavole et al. — Marine heatwave biological impacts.

Chiang and Vimont — North Pacific Meridional Mode definition and dynamics.

Clarke and Van Gorder — Coastal trapped wave dynamics and eastern boundary propagation speeds.

DelSole and Tippett — Analog forecasting and seasonal prediction methods.

Di Lorenzo et al. — North Pacific climate modes and coastal ocean variability.

Enfield and Allen — Coastal Kelvin wave propagation along eastern boundaries.

Huang et al. — OISST dataset description.

Mantua et al. — Pacific Decadal Oscillation.

Newman et al. — PDO dynamics and decomposition.

Romea and Smith — Coastal trapped wave theory and observations.

Small et al. — CESM-HR model description.

Vimont et al. — NPMM dynamics and Pacific climate variability.
