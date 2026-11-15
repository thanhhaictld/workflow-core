# Paired batch-admission result summary

No-028 deployment-matching index set. All times are milliseconds. Each cell has 5 measured samples after 2 warmups. Ranges show min–max; SQL execution is from PostgreSQL EXPLAIN, while client elapsed includes Npgsql execution/roundtrip. `read_samples` reports measured samples with physical reads; reads and slower prototype cells are valid measurements, not verifier failures. WAL is retained, no CPU was measured.

| Stats | Plan | Batch | IDs | Arm | SQL median (min–max) | Planning median | Leased | Buffer hits | Hits / lease | Read samples | Scan node(s) |
|---|---|---:|---|---|---:|---:|---:|---:|---:|---:|---|
| analyzed | custom | 1 | early | baseline | 8.837 (8.204–9.715) | 0.728 | 1 | 1552 | 1552.0 | 0 | Index Scan est=19 actual=2500 removed=47500 hit=1516 read=0 loops=1; Index Scan est=1 actual=1 removed=0 hit=4 read=0 loops=1 |
| analyzed | custom | 1 | early | prototype | 0.265 (0.225–0.311) | 0.687 | 1 | 45 | 45.0 | 0 | Index Scan est=1 actual=1 removed=0 hit=6 read=0 loops=1; Index Scan est=1 actual=1 removed=0 hit=4 read=0 loops=1; Index Scan est=1 actual=1 removed=0 hit=4 read=0 loops=1 |
| analyzed | custom | 1 | late | baseline | 9.032 (8.397–10.456) | 0.838 | 1 | 1553 | 1553.0 | 0 | Index Scan est=19 actual=2500 removed=47500 hit=1515 read=0 loops=1; Index Scan est=1 actual=1 removed=0 hit=4 read=0 loops=1 |
| analyzed | custom | 1 | late | prototype | 0.286 (0.274–0.414) | 0.707 | 1 | 45 | 45.0 | 0 | Index Scan est=1 actual=1 removed=0 hit=6 read=0 loops=1; Index Scan est=1 actual=1 removed=0 hit=4 read=0 loops=1; Index Scan est=1 actual=1 removed=0 hit=4 read=0 loops=1 |
| analyzed | custom | 8 | early | baseline | 9.443 (8.387–11.048) | 0.823 | 8 | 1821 | 227.6 | 0 | Index Scan est=19 actual=2500 removed=47500 hit=1530 read=0 loops=1; Index Scan est=1 actual=1 removed=0 hit=32 read=0 loops=8 |
| analyzed | custom | 8 | early | prototype | 0.766 (0.497–1.254) | 0.740 | 8 | 362 | 45.2 | 0 | Index Scan est=1 actual=1 removed=0 hit=48 read=0 loops=8; Index Scan est=1 actual=1 removed=0 hit=32 read=0 loops=8; Index Scan est=1 actual=1 removed=0 hit=32 read=0 loops=8 |
| analyzed | custom | 8 | late | baseline | 8.626 (7.890–12.089) | 0.703 | 8 | 1819 | 227.4 | 0 | Index Scan est=19 actual=2500 removed=47500 hit=1529 read=0 loops=1; Index Scan est=1 actual=1 removed=0 hit=32 read=0 loops=8 |
| analyzed | custom | 8 | late | prototype | 0.595 (0.496–0.898) | 0.647 | 8 | 366 | 45.8 | 0 | Index Scan est=1 actual=1 removed=0 hit=48 read=0 loops=8; Index Scan est=1 actual=1 removed=0 hit=32 read=0 loops=8; Index Scan est=1 actual=1 removed=0 hit=32 read=0 loops=8 |
| analyzed | custom | 16 | early | baseline | 8.645 (8.434–13.765) | 0.707 | 16 | 2149 | 134.3 | 0 | Index Scan est=20 actual=2500 removed=47500 hit=1545 read=0 loops=1; Index Scan est=1 actual=1 removed=0 hit=64 read=0 loops=16 |
| analyzed | custom | 16 | early | prototype | 1.163 (1.033–1.423) | 0.659 | 16 | 754 | 47.1 | 0 | Index Scan est=1 actual=1 removed=0 hit=96 read=0 loops=16; Index Scan est=1 actual=1 removed=0 hit=64 read=0 loops=16; Index Scan est=1 actual=1 removed=0 hit=64 read=0 loops=16 |
| analyzed | custom | 16 | late | baseline | 8.734 (8.339–9.415) | 0.656 | 16 | 2147 | 134.2 | 0 | Index Scan est=20 actual=2500 removed=47500 hit=1546 read=0 loops=1; Index Scan est=1 actual=1 removed=0 hit=64 read=0 loops=16 |
| analyzed | custom | 16 | late | prototype | 1.026 (0.969–1.173) | 0.667 | 16 | 748 | 46.8 | 0 | Index Scan est=1 actual=1 removed=0 hit=96 read=0 loops=16; Index Scan est=1 actual=1 removed=0 hit=64 read=0 loops=16; Index Scan est=1 actual=1 removed=0 hit=64 read=0 loops=16 |
| analyzed | custom | 32 | early | baseline | 9.602 (8.991–11.687) | 0.917 | 32 | 2770 | 86.6 | 0 | Index Scan est=20 actual=2500 removed=47500 hit=1577 read=0 loops=1; Index Scan est=1 actual=1 removed=0 hit=128 read=0 loops=32 |
| analyzed | custom | 32 | early | prototype | 1.815 (1.610–2.294) | 0.754 | 32 | 1477 | 46.2 | 0 | Index Scan est=1 actual=1 removed=0 hit=192 read=0 loops=32; Index Scan est=1 actual=1 removed=0 hit=128 read=0 loops=32; Index Scan est=1 actual=1 removed=0 hit=128 read=0 loops=32 |
| analyzed | custom | 32 | late | baseline | 10.278 (9.666–10.583) | 0.692 | 32 | 2879 | 90.0 | 0 | Index Scan est=20 actual=2500 removed=47500 hit=1579 read=0 loops=1; Index Scan est=1 actual=1 removed=0 hit=128 read=0 loops=32 |
| analyzed | custom | 32 | late | prototype | 1.721 (1.630–2.659) | 0.588 | 32 | 1493 | 46.7 | 0 | Index Scan est=1 actual=1 removed=0 hit=192 read=0 loops=32; Index Scan est=1 actual=1 removed=0 hit=128 read=0 loops=32; Index Scan est=1 actual=1 removed=0 hit=128 read=0 loops=32 |
| analyzed | generic | 1 | early | baseline | 10.441 (8.330–14.799) | 0.877 | 1 | 1552 | 1552.0 | 0 | Index Scan est=19 actual=2500 removed=47500 hit=1516 read=0 loops=1; Index Scan est=1 actual=1 removed=0 hit=4 read=0 loops=1 |
| analyzed | generic | 1 | early | prototype | 0.346 (0.329–0.428) | 0.955 | 1 | 45 | 45.0 | 0 | Index Scan est=1 actual=1 removed=0 hit=6 read=0 loops=1; Index Scan est=1 actual=1 removed=0 hit=4 read=0 loops=1; Index Scan est=1 actual=1 removed=0 hit=4 read=0 loops=1 |
| analyzed | generic | 1 | late | baseline | 8.681 (7.990–9.439) | 0.729 | 1 | 1551 | 1551.0 | 0 | Index Scan est=19 actual=2500 removed=47500 hit=1515 read=0 loops=1; Index Scan est=1 actual=1 removed=0 hit=4 read=0 loops=1 |
| analyzed | generic | 1 | late | prototype | 0.248 (0.223–0.324) | 0.640 | 1 | 47 | 47.0 | 0 | Index Scan est=1 actual=1 removed=0 hit=6 read=0 loops=1; Index Scan est=1 actual=1 removed=0 hit=4 read=0 loops=1; Index Scan est=1 actual=1 removed=0 hit=4 read=0 loops=1 |
| analyzed | generic | 8 | early | baseline | 8.599 (8.148–9.605) | 0.754 | 8 | 1820 | 227.5 | 0 | Index Scan est=19 actual=2500 removed=47500 hit=1530 read=0 loops=1; Index Scan est=1 actual=1 removed=0 hit=32 read=0 loops=8 |
| analyzed | generic | 8 | early | prototype | 0.586 (0.524–0.753) | 0.628 | 8 | 365 | 45.6 | 0 | Index Scan est=1 actual=1 removed=0 hit=48 read=0 loops=8; Index Scan est=1 actual=1 removed=0 hit=32 read=0 loops=8; Index Scan est=1 actual=1 removed=0 hit=32 read=0 loops=8 |
| analyzed | generic | 8 | late | baseline | 8.040 (8.006–8.957) | 0.641 | 8 | 1826 | 228.2 | 0 | Index Scan est=19 actual=2500 removed=47500 hit=1529 read=0 loops=1; Index Scan est=1 actual=1 removed=0 hit=32 read=0 loops=8 |
| analyzed | generic | 8 | late | prototype | 0.559 (0.486–0.777) | 0.620 | 8 | 364 | 45.5 | 0 | Index Scan est=1 actual=1 removed=0 hit=48 read=0 loops=8; Index Scan est=1 actual=1 removed=0 hit=32 read=0 loops=8; Index Scan est=1 actual=1 removed=0 hit=32 read=0 loops=8 |
| analyzed | generic | 16 | early | baseline | 8.910 (8.520–9.298) | 0.657 | 16 | 2150 | 134.4 | 0 | Index Scan est=20 actual=2500 removed=47500 hit=1545 read=0 loops=1; Index Scan est=1 actual=1 removed=0 hit=64 read=0 loops=16 |
| analyzed | generic | 16 | early | prototype | 1.115 (0.935–1.317) | 0.624 | 16 | 748 | 46.8 | 0 | Index Scan est=1 actual=1 removed=0 hit=96 read=0 loops=16; Index Scan est=1 actual=1 removed=0 hit=64 read=0 loops=16; Index Scan est=1 actual=1 removed=0 hit=64 read=0 loops=16 |
| analyzed | generic | 16 | late | baseline | 9.001 (8.807–10.571) | 0.830 | 16 | 2147 | 134.2 | 0 | Index Scan est=20 actual=2500 removed=47500 hit=1546 read=0 loops=1; Index Scan est=1 actual=1 removed=0 hit=64 read=0 loops=16 |
| analyzed | generic | 16 | late | prototype | 1.020 (0.962–1.249) | 0.659 | 16 | 759 | 47.4 | 0 | Index Scan est=1 actual=1 removed=0 hit=96 read=0 loops=16; Index Scan est=1 actual=1 removed=0 hit=64 read=0 loops=16; Index Scan est=1 actual=1 removed=0 hit=64 read=0 loops=16 |
| analyzed | generic | 32 | early | baseline | 11.188 (9.719–11.720) | 0.842 | 32 | 2838 | 88.7 | 0 | Index Scan est=20 actual=2500 removed=47500 hit=1577 read=0 loops=1; Index Scan est=1 actual=1 removed=0 hit=128 read=0 loops=32 |
| analyzed | generic | 32 | early | prototype | 1.942 (1.854–2.639) | 0.660 | 32 | 1479 | 46.2 | 0 | Index Scan est=1 actual=1 removed=0 hit=192 read=0 loops=32; Index Scan est=1 actual=1 removed=0 hit=128 read=0 loops=32; Index Scan est=1 actual=1 removed=0 hit=128 read=0 loops=32 |
| analyzed | generic | 32 | late | baseline | 9.451 (9.267–11.038) | 0.642 | 32 | 2875 | 89.8 | 0 | Index Scan est=20 actual=2500 removed=47500 hit=1579 read=0 loops=1; Index Scan est=1 actual=1 removed=0 hit=128 read=0 loops=32 |
| analyzed | generic | 32 | late | prototype | 1.722 (1.704–2.767) | 0.622 | 32 | 1483 | 46.3 | 0 | Index Scan est=1 actual=1 removed=0 hit=192 read=0 loops=32; Index Scan est=1 actual=1 removed=0 hit=128 read=0 loops=32; Index Scan est=1 actual=1 removed=0 hit=128 read=0 loops=32 |
| analyzed | npgsql-unprepared | 1 | early | baseline | 8.436 (8.388–9.007) | 0.714 | 1 | 1552 | 1552.0 | 0 | Index Scan est=19 actual=2500 removed=47500 hit=1516 read=0 loops=1; Index Scan est=1 actual=1 removed=0 hit=4 read=0 loops=1 |
| analyzed | npgsql-unprepared | 1 | early | prototype | 0.251 (0.238–0.360) | 0.660 | 1 | 45 | 45.0 | 0 | Index Scan est=1 actual=1 removed=0 hit=6 read=0 loops=1; Index Scan est=1 actual=1 removed=0 hit=4 read=0 loops=1; Index Scan est=1 actual=1 removed=0 hit=4 read=0 loops=1 |
| analyzed | npgsql-unprepared | 1 | late | baseline | 7.746 (7.726–8.734) | 0.681 | 1 | 1551 | 1551.0 | 0 | Index Scan est=19 actual=2500 removed=47500 hit=1515 read=0 loops=1; Index Scan est=1 actual=1 removed=0 hit=4 read=0 loops=1 |
| analyzed | npgsql-unprepared | 1 | late | prototype | 0.233 (0.218–0.265) | 0.641 | 1 | 45 | 45.0 | 0 | Index Scan est=1 actual=1 removed=0 hit=6 read=0 loops=1; Index Scan est=1 actual=1 removed=0 hit=4 read=0 loops=1; Index Scan est=1 actual=1 removed=0 hit=4 read=0 loops=1 |
| analyzed | npgsql-unprepared | 8 | early | baseline | 8.692 (8.218–9.718) | 0.654 | 8 | 1824 | 228.0 | 0 | Index Scan est=19 actual=2500 removed=47500 hit=1530 read=0 loops=1; Index Scan est=1 actual=1 removed=0 hit=32 read=0 loops=8 |
| analyzed | npgsql-unprepared | 8 | early | prototype | 0.608 (0.550–0.732) | 0.620 | 8 | 362 | 45.2 | 0 | Index Scan est=1 actual=1 removed=0 hit=48 read=0 loops=8; Index Scan est=1 actual=1 removed=0 hit=32 read=0 loops=8; Index Scan est=1 actual=1 removed=0 hit=32 read=0 loops=8 |
| analyzed | npgsql-unprepared | 8 | late | baseline | 8.212 (7.879–8.558) | 0.726 | 8 | 1826 | 228.2 | 0 | Index Scan est=19 actual=2500 removed=47500 hit=1529 read=0 loops=1; Index Scan est=1 actual=1 removed=0 hit=32 read=0 loops=8 |
| analyzed | npgsql-unprepared | 8 | late | prototype | 0.630 (0.521–0.861) | 0.685 | 8 | 374 | 46.8 | 0 | Index Scan est=1 actual=1 removed=0 hit=48 read=0 loops=8; Index Scan est=1 actual=1 removed=0 hit=32 read=0 loops=8; Index Scan est=1 actual=1 removed=0 hit=32 read=0 loops=8 |
| analyzed | npgsql-unprepared | 16 | early | baseline | 9.159 (8.149–10.441) | 0.860 | 16 | 2144 | 134.0 | 0 | Index Scan est=20 actual=2500 removed=47500 hit=1545 read=0 loops=1; Index Scan est=1 actual=1 removed=0 hit=64 read=0 loops=16 |
| analyzed | npgsql-unprepared | 16 | early | prototype | 1.030 (0.919–1.200) | 0.631 | 16 | 784 | 49.0 | 0 | Index Scan est=1 actual=1 removed=0 hit=96 read=0 loops=16; Index Scan est=1 actual=1 removed=0 hit=64 read=0 loops=16; Index Scan est=1 actual=1 removed=0 hit=64 read=0 loops=16 |
| analyzed | npgsql-unprepared | 16 | late | baseline | 8.724 (8.476–8.912) | 0.652 | 16 | 2162 | 135.1 | 0 | Index Scan est=20 actual=2500 removed=47500 hit=1546 read=0 loops=1; Index Scan est=1 actual=1 removed=0 hit=64 read=0 loops=16 |
| analyzed | npgsql-unprepared | 16 | late | prototype | 1.103 (0.962–1.177) | 0.577 | 16 | 753 | 47.1 | 0 | Index Scan est=1 actual=1 removed=0 hit=96 read=0 loops=16; Index Scan est=1 actual=1 removed=0 hit=64 read=0 loops=16; Index Scan est=1 actual=1 removed=0 hit=64 read=0 loops=16 |
| analyzed | npgsql-unprepared | 32 | early | baseline | 9.457 (9.154–11.076) | 0.675 | 32 | 2788 | 87.1 | 0 | Index Scan est=20 actual=2500 removed=47500 hit=1577 read=0 loops=1; Index Scan est=1 actual=1 removed=0 hit=128 read=0 loops=32 |
| analyzed | npgsql-unprepared | 32 | early | prototype | 1.906 (1.775–2.128) | 0.631 | 32 | 1480 | 46.2 | 0 | Index Scan est=1 actual=1 removed=0 hit=192 read=0 loops=32; Index Scan est=1 actual=1 removed=0 hit=128 read=0 loops=32; Index Scan est=1 actual=1 removed=0 hit=128 read=0 loops=32 |
| analyzed | npgsql-unprepared | 32 | late | baseline | 9.988 (9.479–11.527) | 0.711 | 32 | 2805 | 87.7 | 0 | Index Scan est=21 actual=2500 removed=47500 hit=1579 read=0 loops=1; Index Scan est=1 actual=1 removed=0 hit=128 read=0 loops=32 |
| analyzed | npgsql-unprepared | 32 | late | prototype | 1.969 (1.927–2.922) | 0.652 | 32 | 1483 | 46.3 | 0 | Index Scan est=1 actual=1 removed=0 hit=192 read=0 loops=32; Index Scan est=1 actual=1 removed=0 hit=128 read=0 loops=32; Index Scan est=1 actual=1 removed=0 hit=128 read=0 loops=32 |
| stale-active | custom | 1 | early | baseline | 13.795 (12.917–14.130) | 0.739 | 1 | 2299 | 2299.0 | 1 | Index Scan est=22 actual=4062 removed=70938 hit=2261 read=0 loops=1; Index Scan est=1 actual=1 removed=0 hit=4 read=0 loops=1 |
| stale-active | custom | 1 | early | prototype | 0.304 (0.280–0.384) | 0.783 | 1 | 47 | 47.0 | 0 | Index Scan est=1 actual=1 removed=0 hit=6 read=0 loops=1; Index Scan est=1 actual=1 removed=0 hit=4 read=0 loops=1; Index Scan est=1 actual=1 removed=0 hit=4 read=0 loops=1 |
| stale-active | custom | 1 | late | baseline | 12.972 (11.686–14.969) | 0.684 | 1 | 2294 | 2294.0 | 1 | Index Scan est=23 actual=4062 removed=70938 hit=2259 read=0 loops=1; Index Scan est=1 actual=1 removed=0 hit=4 read=0 loops=1 |
| stale-active | custom | 1 | late | prototype | 0.221 (0.199–0.263) | 0.591 | 1 | 43 | 43.0 | 0 | Index Scan est=1 actual=1 removed=0 hit=5 read=0 loops=1; Index Scan est=1 actual=1 removed=0 hit=4 read=0 loops=1; Index Scan est=1 actual=1 removed=0 hit=4 read=0 loops=1 |
| stale-active | custom | 8 | early | baseline | 12.283 (11.639–27.208) | 0.858 | 8 | 2586 | 323.2 | 0 | Index Scan est=23 actual=4062 removed=70938 hit=2275 read=0 loops=1; Index Scan est=1 actual=1 removed=0 hit=32 read=0 loops=8 |
| stale-active | custom | 8 | early | prototype | 0.677 (0.562–0.733) | 0.635 | 8 | 381 | 47.6 | 0 | Index Scan est=1 actual=1 removed=0 hit=48 read=0 loops=8; Index Scan est=1 actual=1 removed=0 hit=32 read=0 loops=8; Index Scan est=1 actual=1 removed=0 hit=32 read=0 loops=8 |
| stale-active | custom | 8 | late | baseline | 11.916 (11.762–13.034) | 0.604 | 8 | 2563 | 320.4 | 0 | Index Scan est=23 actual=4062 removed=70938 hit=2273 read=0 loops=1; Index Scan est=1 actual=1 removed=0 hit=32 read=0 loops=8 |
| stale-active | custom | 8 | late | prototype | 0.583 (0.516–0.626) | 0.612 | 8 | 361 | 45.1 | 0 | Index Scan est=1 actual=1 removed=0 hit=47 read=0 loops=8; Index Scan est=1 actual=1 removed=0 hit=32 read=0 loops=8; Index Scan est=1 actual=1 removed=0 hit=32 read=0 loops=8 |
| stale-active | custom | 16 | early | baseline | 14.372 (12.246–16.520) | 0.749 | 16 | 2921 | 182.6 | 0 | Index Scan est=23 actual=4062 removed=70938 hit=2290 read=0 loops=1; Index Scan est=1 actual=1 removed=0 hit=64 read=0 loops=16 |
| stale-active | custom | 16 | early | prototype | 1.231 (1.011–1.388) | 0.661 | 16 | 774 | 48.4 | 0 | Index Scan est=1 actual=1 removed=0 hit=96 read=0 loops=16; Index Scan est=1 actual=1 removed=0 hit=64 read=0 loops=16; Index Scan est=1 actual=1 removed=0 hit=64 read=0 loops=16 |
| stale-active | custom | 16 | late | baseline | 12.485 (12.161–12.955) | 0.670 | 16 | 2906 | 181.6 | 0 | Index Scan est=23 actual=4062 removed=70938 hit=2289 read=0 loops=1; Index Scan est=1 actual=1 removed=0 hit=64 read=0 loops=16 |
| stale-active | custom | 16 | late | prototype | 1.051 (1.021–1.336) | 0.619 | 16 | 761 | 47.6 | 0 | Index Scan est=1 actual=1 removed=0 hit=95 read=0 loops=16; Index Scan est=1 actual=1 removed=0 hit=64 read=0 loops=16; Index Scan est=1 actual=1 removed=0 hit=64 read=0 loops=16 |
| stale-active | custom | 32 | early | baseline | 12.955 (12.640–14.034) | 0.696 | 32 | 3557 | 111.2 | 0 | Index Scan est=23 actual=4062 removed=70938 hit=2322 read=0 loops=1; Index Scan est=1 actual=1 removed=0 hit=128 read=0 loops=32 |
| stale-active | custom | 32 | early | prototype | 1.784 (1.661–1.905) | 0.593 | 32 | 1521 | 47.5 | 0 | Index Scan est=1 actual=1 removed=0 hit=192 read=0 loops=32; Index Scan est=1 actual=1 removed=0 hit=128 read=0 loops=32; Index Scan est=1 actual=1 removed=0 hit=128 read=0 loops=32 |
| stale-active | custom | 32 | late | baseline | 13.165 (12.811–14.365) | 0.642 | 32 | 3561 | 111.3 | 0 | Index Scan est=23 actual=4062 removed=70938 hit=2324 read=0 loops=1; Index Scan est=1 actual=1 removed=0 hit=128 read=0 loops=32 |
| stale-active | custom | 32 | late | prototype | 1.873 (1.767–2.028) | 0.640 | 32 | 1533 | 47.9 | 0 | Index Scan est=1 actual=1 removed=0 hit=192 read=0 loops=32; Index Scan est=1 actual=1 removed=0 hit=128 read=0 loops=32; Index Scan est=1 actual=1 removed=0 hit=128 read=0 loops=32 |
| stale-active | generic | 1 | early | baseline | 11.711 (11.346–11.958) | 0.611 | 1 | 2299 | 2299.0 | 0 | Index Scan est=22 actual=4062 removed=70938 hit=2261 read=0 loops=1; Index Scan est=1 actual=1 removed=0 hit=4 read=0 loops=1 |
| stale-active | generic | 1 | early | prototype | 0.235 (0.212–0.249) | 0.590 | 1 | 47 | 47.0 | 0 | Index Scan est=1 actual=1 removed=0 hit=6 read=0 loops=1; Index Scan est=1 actual=1 removed=0 hit=4 read=0 loops=1; Index Scan est=1 actual=1 removed=0 hit=4 read=0 loops=1 |
| stale-active | generic | 1 | late | baseline | 12.472 (11.412–15.172) | 0.644 | 1 | 2294 | 2294.0 | 0 | Index Scan est=23 actual=4062 removed=70938 hit=2259 read=0 loops=1; Index Scan est=1 actual=1 removed=0 hit=4 read=0 loops=1 |
| stale-active | generic | 1 | late | prototype | 0.216 (0.208–0.327) | 0.605 | 1 | 43 | 43.0 | 0 | Index Scan est=1 actual=1 removed=0 hit=5 read=0 loops=1; Index Scan est=1 actual=1 removed=0 hit=4 read=0 loops=1; Index Scan est=1 actual=1 removed=0 hit=4 read=0 loops=1 |
| stale-active | generic | 8 | early | baseline | 12.200 (11.727–12.805) | 0.657 | 8 | 2583 | 322.9 | 0 | Index Scan est=23 actual=4062 removed=70938 hit=2275 read=0 loops=1; Index Scan est=1 actual=1 removed=0 hit=32 read=0 loops=8 |
| stale-active | generic | 8 | early | prototype | 0.603 (0.567–0.652) | 0.612 | 8 | 383 | 47.9 | 0 | Index Scan est=1 actual=1 removed=0 hit=48 read=0 loops=8; Index Scan est=1 actual=1 removed=0 hit=32 read=0 loops=8; Index Scan est=1 actual=1 removed=0 hit=32 read=0 loops=8 |
| stale-active | generic | 8 | late | baseline | 12.325 (11.826–15.458) | 0.644 | 8 | 2566 | 320.8 | 0 | Index Scan est=23 actual=4062 removed=70938 hit=2273 read=0 loops=1; Index Scan est=1 actual=1 removed=0 hit=32 read=0 loops=8 |
| stale-active | generic | 8 | late | prototype | 0.661 (0.575–0.906) | 0.625 | 8 | 364 | 45.5 | 0 | Index Scan est=1 actual=1 removed=0 hit=47 read=0 loops=8; Index Scan est=1 actual=1 removed=0 hit=32 read=0 loops=8; Index Scan est=1 actual=1 removed=0 hit=32 read=0 loops=8 |
| stale-active | generic | 16 | early | baseline | 12.692 (12.197–12.925) | 0.687 | 16 | 2916 | 182.2 | 0 | Index Scan est=23 actual=4062 removed=70938 hit=2290 read=0 loops=1; Index Scan est=1 actual=1 removed=0 hit=64 read=0 loops=16 |
| stale-active | generic | 16 | early | prototype | 1.139 (1.016–1.267) | 0.684 | 16 | 776 | 48.5 | 0 | Index Scan est=1 actual=1 removed=0 hit=96 read=0 loops=16; Index Scan est=1 actual=1 removed=0 hit=64 read=0 loops=16; Index Scan est=1 actual=1 removed=0 hit=64 read=0 loops=16 |
| stale-active | generic | 16 | late | baseline | 12.381 (12.180–13.073) | 0.649 | 16 | 2911 | 181.9 | 0 | Index Scan est=23 actual=4062 removed=70938 hit=2289 read=0 loops=1; Index Scan est=1 actual=1 removed=0 hit=64 read=0 loops=16 |
| stale-active | generic | 16 | late | prototype | 1.000 (0.868–1.276) | 0.635 | 16 | 755 | 47.2 | 0 | Index Scan est=1 actual=1 removed=0 hit=95 read=0 loops=16; Index Scan est=1 actual=1 removed=0 hit=64 read=0 loops=16; Index Scan est=1 actual=1 removed=0 hit=64 read=0 loops=16 |
| stale-active | generic | 32 | early | baseline | 13.198 (12.657–14.929) | 0.684 | 32 | 3564 | 111.4 | 0 | Index Scan est=23 actual=4062 removed=70938 hit=2322 read=0 loops=1; Index Scan est=1 actual=1 removed=0 hit=128 read=0 loops=32 |
| stale-active | generic | 32 | early | prototype | 1.855 (1.582–2.732) | 0.650 | 32 | 1534 | 47.9 | 0 | Index Scan est=1 actual=1 removed=0 hit=192 read=0 loops=32; Index Scan est=1 actual=1 removed=0 hit=128 read=0 loops=32; Index Scan est=1 actual=1 removed=0 hit=128 read=0 loops=32 |
| stale-active | generic | 32 | late | baseline | 13.159 (12.641–14.086) | 0.665 | 32 | 3588 | 112.1 | 0 | Index Scan est=24 actual=4062 removed=70938 hit=2324 read=0 loops=1; Index Scan est=1 actual=1 removed=0 hit=128 read=0 loops=32 |
| stale-active | generic | 32 | late | prototype | 1.704 (1.660–1.890) | 0.616 | 32 | 1522 | 47.6 | 0 | Index Scan est=1 actual=1 removed=0 hit=192 read=0 loops=32; Index Scan est=1 actual=1 removed=0 hit=128 read=0 loops=32; Index Scan est=1 actual=1 removed=0 hit=128 read=0 loops=32 |
| stale-active | npgsql-unprepared | 1 | early | baseline | 11.690 (11.528–14.801) | 0.704 | 1 | 2299 | 2299.0 | 0 | Index Scan est=23 actual=4062 removed=70938 hit=2261 read=0 loops=1; Index Scan est=1 actual=1 removed=0 hit=4 read=0 loops=1 |
| stale-active | npgsql-unprepared | 1 | early | prototype | 0.232 (0.205–0.267) | 0.588 | 1 | 47 | 47.0 | 0 | Index Scan est=1 actual=1 removed=0 hit=6 read=0 loops=1; Index Scan est=1 actual=1 removed=0 hit=4 read=0 loops=1; Index Scan est=1 actual=1 removed=0 hit=4 read=0 loops=1 |
| stale-active | npgsql-unprepared | 1 | late | baseline | 12.347 (11.467–13.870) | 0.684 | 1 | 2295 | 2295.0 | 0 | Index Scan est=23 actual=4062 removed=70938 hit=2259 read=0 loops=1; Index Scan est=1 actual=1 removed=0 hit=4 read=0 loops=1 |
| stale-active | npgsql-unprepared | 1 | late | prototype | 0.230 (0.228–0.267) | 0.593 | 1 | 43 | 43.0 | 0 | Index Scan est=1 actual=1 removed=0 hit=5 read=0 loops=1; Index Scan est=1 actual=1 removed=0 hit=4 read=0 loops=1; Index Scan est=1 actual=1 removed=0 hit=4 read=0 loops=1 |
| stale-active | npgsql-unprepared | 8 | early | baseline | 12.943 (12.062–13.559) | 0.692 | 8 | 2590 | 323.8 | 0 | Index Scan est=23 actual=4062 removed=70938 hit=2275 read=0 loops=1; Index Scan est=1 actual=1 removed=0 hit=32 read=0 loops=8 |
| stale-active | npgsql-unprepared | 8 | early | prototype | 0.696 (0.581–0.715) | 0.660 | 8 | 383 | 47.9 | 0 | Index Scan est=1 actual=1 removed=0 hit=48 read=0 loops=8; Index Scan est=1 actual=1 removed=0 hit=32 read=0 loops=8; Index Scan est=1 actual=1 removed=0 hit=32 read=0 loops=8 |
| stale-active | npgsql-unprepared | 8 | late | baseline | 12.338 (12.016–13.644) | 0.650 | 8 | 2563 | 320.4 | 0 | Index Scan est=23 actual=4062 removed=70938 hit=2273 read=0 loops=1; Index Scan est=1 actual=1 removed=0 hit=32 read=0 loops=8 |
| stale-active | npgsql-unprepared | 8 | late | prototype | 0.598 (0.580–0.695) | 0.633 | 8 | 361 | 45.1 | 0 | Index Scan est=1 actual=1 removed=0 hit=47 read=0 loops=8; Index Scan est=1 actual=1 removed=0 hit=32 read=0 loops=8; Index Scan est=1 actual=1 removed=0 hit=32 read=0 loops=8 |
| stale-active | npgsql-unprepared | 16 | early | baseline | 13.140 (12.254–14.125) | 0.657 | 16 | 2926 | 182.9 | 0 | Index Scan est=23 actual=4062 removed=70938 hit=2290 read=0 loops=1; Index Scan est=1 actual=1 removed=0 hit=64 read=0 loops=16 |
| stale-active | npgsql-unprepared | 16 | early | prototype | 1.098 (1.017–1.729) | 0.669 | 16 | 766 | 47.9 | 0 | Index Scan est=1 actual=1 removed=0 hit=96 read=0 loops=16; Index Scan est=1 actual=1 removed=0 hit=64 read=0 loops=16; Index Scan est=1 actual=1 removed=0 hit=64 read=0 loops=16 |
| stale-active | npgsql-unprepared | 16 | late | baseline | 12.733 (12.335–14.450) | 0.634 | 16 | 2903 | 181.4 | 0 | Index Scan est=23 actual=4062 removed=70938 hit=2289 read=0 loops=1; Index Scan est=1 actual=1 removed=0 hit=64 read=0 loops=16 |
| stale-active | npgsql-unprepared | 16 | late | prototype | 1.017 (0.907–1.141) | 0.651 | 16 | 763 | 47.7 | 0 | Index Scan est=1 actual=1 removed=0 hit=95 read=0 loops=16; Index Scan est=1 actual=1 removed=0 hit=64 read=0 loops=16; Index Scan est=1 actual=1 removed=0 hit=64 read=0 loops=16 |
| stale-active | npgsql-unprepared | 32 | early | baseline | 13.033 (12.879–13.462) | 0.643 | 32 | 3590 | 112.2 | 0 | Index Scan est=23 actual=4062 removed=70938 hit=2322 read=0 loops=1; Index Scan est=1 actual=1 removed=0 hit=128 read=0 loops=32 |
| stale-active | npgsql-unprepared | 32 | early | prototype | 1.984 (1.617–2.681) | 0.668 | 32 | 1529 | 47.8 | 0 | Index Scan est=1 actual=1 removed=0 hit=192 read=0 loops=32; Index Scan est=1 actual=1 removed=0 hit=128 read=0 loops=32; Index Scan est=1 actual=1 removed=0 hit=128 read=0 loops=32 |
| stale-active | npgsql-unprepared | 32 | late | baseline | 13.855 (12.934–17.194) | 0.692 | 32 | 3657 | 114.3 | 0 | Index Scan est=24 actual=4062 removed=70938 hit=2324 read=0 loops=1; Index Scan est=1 actual=1 removed=0 hit=128 read=0 loops=32 |
| stale-active | npgsql-unprepared | 32 | late | prototype | 1.899 (1.763–1.922) | 0.625 | 32 | 1520 | 47.5 | 0 | Index Scan est=1 actual=1 removed=0 hit=192 read=0 loops=32; Index Scan est=1 actual=1 removed=0 hit=128 read=0 loops=32; Index Scan est=1 actual=1 removed=0 hit=128 read=0 loops=32 |

## Paired SQL execution deltas

Percent change is prototype vs baseline on the same row set and statistics condition; negative is faster, positive is slower. Results are descriptive; a nonwin or a physical read is a measurement, not an integrity failure.

| Stats | Plan | Batch | IDs | Baseline median ms | Prototype median ms | Change | Baseline hits/lease | Prototype hits/lease |
|---|---|---:|---|---:|---:|---:|---:|---:|
| stale-active | npgsql-unprepared | 1 | early | 11.690 | 0.232 | -98.0% | 2299.0 | 47.0 |
| stale-active | npgsql-unprepared | 1 | late | 12.347 | 0.230 | -98.1% | 2295.0 | 43.0 |
| stale-active | npgsql-unprepared | 8 | early | 12.943 | 0.696 | -94.6% | 323.8 | 47.9 |
| stale-active | npgsql-unprepared | 8 | late | 12.338 | 0.598 | -95.2% | 320.4 | 45.1 |
| stale-active | npgsql-unprepared | 16 | early | 13.140 | 1.098 | -91.6% | 182.9 | 47.9 |
| stale-active | npgsql-unprepared | 16 | late | 12.733 | 1.017 | -92.0% | 181.4 | 47.7 |
| stale-active | npgsql-unprepared | 32 | early | 13.033 | 1.984 | -84.8% | 112.2 | 47.8 |
| stale-active | npgsql-unprepared | 32 | late | 13.855 | 1.899 | -86.3% | 114.3 | 47.5 |
| stale-active | custom | 1 | early | 13.795 | 0.304 | -97.8% | 2299.0 | 47.0 |
| stale-active | custom | 1 | late | 12.972 | 0.221 | -98.3% | 2294.0 | 43.0 |
| stale-active | custom | 8 | early | 12.283 | 0.677 | -94.5% | 323.2 | 47.6 |
| stale-active | custom | 8 | late | 11.916 | 0.583 | -95.1% | 320.4 | 45.1 |
| stale-active | custom | 16 | early | 14.372 | 1.231 | -91.4% | 182.6 | 48.4 |
| stale-active | custom | 16 | late | 12.485 | 1.051 | -91.6% | 181.6 | 47.6 |
| stale-active | custom | 32 | early | 12.955 | 1.784 | -86.2% | 111.2 | 47.5 |
| stale-active | custom | 32 | late | 13.165 | 1.873 | -85.8% | 111.3 | 47.9 |
| stale-active | generic | 1 | early | 11.711 | 0.235 | -98.0% | 2299.0 | 47.0 |
| stale-active | generic | 1 | late | 12.472 | 0.216 | -98.3% | 2294.0 | 43.0 |
| stale-active | generic | 8 | early | 12.200 | 0.603 | -95.1% | 322.9 | 47.9 |
| stale-active | generic | 8 | late | 12.325 | 0.661 | -94.6% | 320.8 | 45.5 |
| stale-active | generic | 16 | early | 12.692 | 1.139 | -91.0% | 182.2 | 48.5 |
| stale-active | generic | 16 | late | 12.381 | 1.000 | -91.9% | 181.9 | 47.2 |
| stale-active | generic | 32 | early | 13.198 | 1.855 | -85.9% | 111.4 | 47.9 |
| stale-active | generic | 32 | late | 13.159 | 1.704 | -87.1% | 112.1 | 47.6 |
| analyzed | npgsql-unprepared | 1 | early | 8.436 | 0.251 | -97.0% | 1552.0 | 45.0 |
| analyzed | npgsql-unprepared | 1 | late | 7.746 | 0.233 | -97.0% | 1551.0 | 45.0 |
| analyzed | npgsql-unprepared | 8 | early | 8.692 | 0.608 | -93.0% | 228.0 | 45.2 |
| analyzed | npgsql-unprepared | 8 | late | 8.212 | 0.630 | -92.3% | 228.2 | 46.8 |
| analyzed | npgsql-unprepared | 16 | early | 9.159 | 1.030 | -88.8% | 134.0 | 49.0 |
| analyzed | npgsql-unprepared | 16 | late | 8.724 | 1.103 | -87.4% | 135.1 | 47.1 |
| analyzed | npgsql-unprepared | 32 | early | 9.457 | 1.906 | -79.8% | 87.1 | 46.2 |
| analyzed | npgsql-unprepared | 32 | late | 9.988 | 1.969 | -80.3% | 87.7 | 46.3 |
| analyzed | custom | 1 | early | 8.837 | 0.265 | -97.0% | 1552.0 | 45.0 |
| analyzed | custom | 1 | late | 9.032 | 0.286 | -96.8% | 1553.0 | 45.0 |
| analyzed | custom | 8 | early | 9.443 | 0.766 | -91.9% | 227.6 | 45.2 |
| analyzed | custom | 8 | late | 8.626 | 0.595 | -93.1% | 227.4 | 45.8 |
| analyzed | custom | 16 | early | 8.645 | 1.163 | -86.5% | 134.3 | 47.1 |
| analyzed | custom | 16 | late | 8.734 | 1.026 | -88.3% | 134.2 | 46.8 |
| analyzed | custom | 32 | early | 9.602 | 1.815 | -81.1% | 86.6 | 46.2 |
| analyzed | custom | 32 | late | 10.278 | 1.721 | -83.3% | 90.0 | 46.7 |
| analyzed | generic | 1 | early | 10.441 | 0.346 | -96.7% | 1552.0 | 45.0 |
| analyzed | generic | 1 | late | 8.681 | 0.248 | -97.1% | 1551.0 | 47.0 |
| analyzed | generic | 8 | early | 8.599 | 0.586 | -93.2% | 227.5 | 45.6 |
| analyzed | generic | 8 | late | 8.040 | 0.559 | -93.0% | 228.2 | 45.5 |
| analyzed | generic | 16 | early | 8.910 | 1.115 | -87.5% | 134.4 | 46.8 |
| analyzed | generic | 16 | late | 9.001 | 1.020 | -88.7% | 134.2 | 47.4 |
| analyzed | generic | 32 | early | 11.188 | 1.942 | -82.6% | 88.7 | 46.2 |
| analyzed | generic | 32 | late | 9.451 | 1.722 | -81.8% | 89.8 | 46.3 |

Integrity/outcome summary: 96 arm cells; median nonwins/equalities=0; samples with physical reads=2. Neither outcome invalidates a cell.
