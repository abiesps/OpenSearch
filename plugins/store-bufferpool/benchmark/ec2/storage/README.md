# Storage characterization (EBS gp3 vs EFS) for the cold-path POC

Builds the storage model the prefetch and IO-size constants come from (common-rules.md, IO configuration).
Runs as root on one data node; access through SSM only.

| File | What it does |
|---|---|
| `setup.sh` | formats the EBS data volume (XFS, noatime), mounts EFS with amazon-efs-utils (TLS), installs fio/gcc/JDK, records the facts |
| `prepare.sh` | builds `wnread`, checks the tracepoints, writes a 64 GiB random-data file on each storage |
| `wnread.c` | the bufferpool's read call sequence (POSIX_FADV_WILLNEED of the window on a second fd, then one pread) and an mmap mode (MADV_NORMAL / MADV_RANDOM) for stock MMapDirectory; one thread per in-flight read, disjoint regions |
| `storbench.py` | the matrix: regimes x sizes x QD 1-64 x repetitions, page cache dropped per job, readahead set before open and sampled every 100 ms, ftrace request-size histogram (block_rq_issue / nfs_initiate_read), mountstats / diskstats deltas |
| `storbench.py --plan plan-*.json --block <name>` | per-curve queue depths (knee and stability blocks); regions are 1 MiB aligned for every QD |
| `model.py` | per-block, per-curve medians; one knee rule (highest QD with p50 <= 1.10 x the block's QD 1 p50 at it and every lower QD); Little's law; HTML tables |
| `equalwork.py` | acceptance check per job: >= 99 % of device requests at the expected size, 1.05 <= device bytes / tool bytes <= 1.30 for non-amplifying regimes |
| `rateprobe.py` | one job with the device request trace binned per 100 ms (explains device bytes outside the measured window) |
| `bp_setup.sh`, `bp_restore.sh` | POC binary units for EBS and EFS, restore of a stock snapshot with index.store.type=bufferpoolfs |
| `bpload.py` | bufferpool load latency per window from `/_bufferpool/stats` over cold workload ops, with the shared device check (`harness/agent/coldpath_readattr.py`) and `drop_until_empty` (`harness/agent/coldpath_agent.py`) |
| `bpmodel.py` | load latency per storage and window size |

fio's mmap engine is not used: with `numjobs` on one file it ignores `offset_increment`, so every job reads the same
pages (device bytes far below fio bytes).
