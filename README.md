# boom_run

## feature:
方便在crontab里执行命令, 出问题可报警，附带分布式锁

## To Do List:
1. 间隔性的网络锁
2. 加入本地锁
3. 加入shell exit code判断
4. syslog输出

## usage:

```
crontab -e

*/20 * * * * boom_run --mail "rfyiamcool@163.com;rfyiamcool@samsung.com" --timeout=15 "python /dev/app/ns_scan.py 2>&1"
```
