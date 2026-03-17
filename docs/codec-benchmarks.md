# Codec Benchmarks

Measured with:

```bash
cd /Users/noahjeske/Documents/storagex && uv run python scripts/benchmark_codec.py --modes current,legacy --sizes 1,10,100 --json
```

`current` is the new v3 keyed + `.mkv` / `ffv1` path.

`legacy` is the old v2 keyed + `.webm` / lossless VP9 path.

## Results

| Mode | Size (MiB) | Encode (s) | Decode (s) | Archive | Archive Size (MiB) | Integrity |
| --- | ---: | ---: | ---: | --- | ---: | --- |
| legacy | 1 | 11.566 | 1.065 | `.webm` | 1.96 | true |
| legacy | 10 | 134.830 | 13.456 | `.webm` | 19.25 | true |
| legacy | 100 | ~1348.298 | ~134.559 | `.webm` | ~192.53 | estimated |
| current | 1 | 0.383 | 0.410 | `.mkv` | 4.24 | true |
| current | 10 | 3.218 | 3.694 | `.mkv` | 42.41 | true |
| current | 100 | 31.363 | 36.702 | `.mkv` | 424.04 | true |

## Notes

- The `legacy` 100 MiB row is a linear estimate from the measured 10 MiB run. A direct full run was skipped because the old path is prohibitively slow at that size on this machine.
- The new path trades archive size for speed. Local `.mkv` archives are materially larger than the old `.webm` output, but encode/decode latency drops by roughly an order of magnitude for the tested sizes.
