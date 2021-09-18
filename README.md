# containerScheduler
天池首届云原生编程挑战赛亚军-初赛赛道二（实现规模化容器静态布局和动态迁移）方案

初赛排名（8/4031） schedule(249828)  reschedule(254093)  total(503921)

## 算法思路

算法思路比较简单，schedule和reschedule都是整体模拟退火，局部贪心思路。具体方案思路在documents

搞的时间比较短，代码写的比较乱，主要是最后为了拿个手机，疯狂改代码，太卷了，后续有时间可能会重构一下。

重点代码在django-calculate里，文件结构如下：

```
.
├── AlloPod.java						
├── Allocation.java
├── AllocationStrategy.java       scheduler入口
├── CalculateReschedule.java      reschedule主体代码
├── CalculateSchedule.java        schedule主体代码
├── DPTrail.java
├── MigratePod.java
├── NodeAndPod.java               节点信息（包括部署的Pods）
├── PendingMigratePod.java
└── Utils.java                    工具类，主要是判断资源等约束条件
```

