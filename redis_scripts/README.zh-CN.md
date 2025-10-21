| Key                           | Redis 数据结构 | 用途                                                                          |
| ----------------------------- | -------------- | ----------------------------------------------------------------------------- |
| easy-mq:topics                | Set            | 存储当前存在的 topic 列表。member: topic1, topic2 ...                         |
| easy-mq:task                  | Hash           | 存储任务的元数据,运行时数据。 key: task_id fields: 任务元数据,运行时数据...   |
| easy-mq:{topic}:pending       | SortedSet      | 等待消费的任务列表。 score: 优先级, member: task_id                           |
| easy-mq:{topic}:scheduled     | SortedSet      | 指定时间才可以开始被消费的任务列表。 score: 指定的消费时间, member: task_id   |
| easy-mq:{topic}:dep:{task_id} | Set            | 等待其他任务执行完毕才可以开始被消费的任务列表。member: 依赖的任务 task_id... |
| easy-mq:{topic}:active        | SortedSet      | 处于消费中的任务列表。 score: 任务出队(被消费)时间, member: task_id           |
| easy-mq:{topic}:completed     | SortedSet      | 执行完毕的任务列表。 score: 任务完成时间, member: task_id                     |
