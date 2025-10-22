| Key                                    | Redis 数据结构 | 用途                                                                                                   |
| -------------------------------------- | -------------- | ------------------------------------------------------------------------------------------------------ |
| easy-mq:topics                         | SortedSet      | 存储当前存在的 topic 列表。score: topic 最近入队时间。 member: topic 名称                              |
| easy-mq:task:{task_id}                 | Hash           | 存储任务的元数据,运行时数据。                                                                          |
| easy-mq:stream:{topic}:{priority}      | Stream         | 等待消费的任务队列。 filed: task_id; 默认使用 group `global`, consumer `global`                        |
| easy-mq:scheduled                      | SortedSet      | 指定时间才可以开始被消费的任务列表。 score: 指定的消费时间; member: task_id                            |
| easy-mq:dependence                     | SortedSet      | 存储需要依赖其他任务执行完毕/成功/失败/被取消的任务列表。 score: 存储时间; member: task_id             |
| easy-mq:archive                        | SortedSet      | 存储任务执行完毕的 task_id(无论成功,失败,被取消)。 score: 存档时间; member: task_id                    |
| easy-mq:dep:{task_id}                  | Set            | 存储任务所依赖的其他任务。 member: {task_state}/{task_id} (task_state 为任务执行完毕/成功/失败/被取消) |
| easy-mq:rev-dep:{task_state}/{task_id} | Set            | 存储被其他任务依赖的任务。 member: 被依赖的 task_id                                                    |
