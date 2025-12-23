# ARI 计算服务

功能：
- 从 ClickHouse 读取分钟级传感器数据
- 每 30 分钟计算一次 ARI1 ~ ARI5
- 写回 ARI 结果表

运行：
python main.py