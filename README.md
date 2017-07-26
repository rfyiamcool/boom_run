# boom_run

## To Do List:
1. 间隔性的网络锁
2. 加入本地锁
3. 加入shell exit code判断

## usage:

```
crontab -e

*/20 * * * * boom_run --mail "rfyiamcool@163.com;rfyiamcool@samsung.com" --timeout=15 "python /dev/app/ns_scan.py 2>&1"
```