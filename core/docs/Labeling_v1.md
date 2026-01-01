# Labeling v1 Specification

**Version**: 1.0.0
**Default Target**: Binary Directional (10s Horizon)

## 1. Label Definition
Prediction of whether the mid-price will be higher exactly 10 seconds in the future.

- **Formula**:
  `label_dir_10s = 1 if mid[t + 10s] > mid[t] else 0`
- **Horizon ($\Delta$)**: 10 seconds.

## 2. Alignment Rules (Determinism)
To avoid lookahead bias while ensuring a realistic target, we use **Nearest Next Event (Forward)** selection for the target price.

1. **Target Timestamp**: $T_{target} = ts\_event[t] + 10,000ms$.
2. **Event Selection**: Use **"The first event where $ts\_event \ge T_{target}$"**.
   - *Rationale*: This represents the first actionable price update available to the trader after the 10-second mark.
3. **Sequence Handling**: If multiple events have the same $T_{target}$, the one with the smallest `seq` (first arrival) is used.

## 3. Split & Leakage Rules
### Data Split (Time-Based)
Training on a single day requires strict chronological partitioning:
- **Train**: First 70% of chronological events.
- **Validation**: Next 15% of events.
- **Test**: Last 15% of events.

### Leakage Prevention
1. **Horizon Drop**: The **last 10 seconds** of the dataset MUST be dropped before training. These samples do not have a valid label as $T_{target}$ exceeds the available data range.
2. **Lookahead Audit**: Features at time $t$ MUST ONLY use data where $ts\_event \le ts\_event[t]$.

## 4. Feature-Label Alignment (Visual)
```text
Event t: [Mid=100.0]  (ts=1000)
...
Looking for event >= 1000 + 10000 (11000):
- Event A at ts=10950 [IGNORED]
- Event B at ts=11005 [SELECTED] (Mid=100.5)
- Label = 1 (100.5 > 100.0)
```

## 5. NaN Handling
- Rows with `label_dir_10s = NaN` (due to end of day or data gaps) are dropped.
- Cold start rows (where rolling features are not yet ready) are dropped.
 Linda v1’de multi-horizon pasif.
