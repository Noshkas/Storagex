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
| current | 1 | 0.074 | 0.108 | `.mkv` | 1.07 | true |
| current | 10 | 0.236 | 0.252 | `.mkv` | 10.64 | true |
| current | 100 | 2.012 | 1.842 | `.mkv` | 106.35 | true |

## Notes

- The `legacy` 100 MiB row is a linear estimate from the measured 10 MiB run. A direct full run was skipped because the old path is prohibitively slow at that size on this machine.
- The new default local path uses dense raw-byte frames inside lossless `.mkv`, so frame count is dramatically lower than the bit-grid path. The bit-grid layout is still used for YouTube-safe archives and opt-in debug PNG artifacts.
- The local `.mkv` archive is now close to payload size instead of expanding it heavily, while still decoding much faster than the old `.webm` path.
