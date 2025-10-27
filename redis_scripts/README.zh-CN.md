| Key                                     | Redis 数据结构 | 用途                                                                                                 |
| --------------------------------------- | -------------- | ---------------------------------------------------------------------------------------------------- |
| easy-mq:topics                          | SortedSet      | 存储当前活跃的 `topic`. score: 最后产生任务的时间; member: `topic`                                   |
| easy-mq:{`topic`}:qname                 | Hash           | 存储 topic 下存在的队列名称. key: 队列名称; value: 更新时间                                          |
| easy-mq:{`qname`}:task:{`task_id`}      | Hash           | 存储任务数据,状态等. fields: 任务数据,重试次数,`stream_id`,`state`,`timeout` 等                      |
| easy-mq:{`qname`}:stream                | Stream         | 存储 Pending 任务队列. 该队列下的任务可以直接被消费. 默认 group `default`; consumer `default`        |
| easy-mq:{`qname`}:scheduled             | SortedSet      | 存储定时任务. score: 指定时间; member: `task_key` (easy-mq:{qname}:task:{task_id})                   |
| easy-mq:{`qname`}:dependent             | Set            | 存储依赖其他任务执行完毕的任务集. member: `dependent_task_key` (easy-mq:{qname}:dependent:{task_id}) |
| easy-mq:{`qname`}:dependent:{`task_id`} | Hash           | 存储依赖其他任务执行完毕的任务. key: 依赖任务的`task_key`; value: 任务完成状态                       |
| easy-mq:{`qname`}:archive               | SortedSet      | 存储执行完毕的任务. score: 完成时间; member: `task_key`                                              |
| easy-mq:{`qname`}:deadline              | SortedSet      | 存储有截止时间的任务. score: 截止时间; member: `task_key`                                            |
