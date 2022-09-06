# amq-rabbit

# AMQ 使用示例

使用 amq 消息模型进行通信的服务，需要一个实现了 `amq.Processor` 接口的类用于监听，处理特定消息。

`msg.SetType` 所指定的类型会被相同类型的 `Procosser` 处理。

## 通知消息
适用于简单收发消息场景，见 `notice-message` 给出的示例代码。

运行示例：
```shell script
cd example
make run_notice_sender
make run_notice_receiver
```

## 单向事务消息
此消息模型可用于实现异步应答机制。
`simplex-message` 包含一个简单单向事务消息示例，其模型如下图：
![image](https://user-images.githubusercontent.com/11751256/188566950-ecb38f68-13ad-48f1-b199-2675b0aec270.png)

运行示例代码：
```shell script
cd example
# 运行发送方示例，使用 ctrl+C 退出
make run_simplex_sender
# 运行接受者示例，使用 ctrl+C 退出
make run_simplex_receiver
```

## 双向事务消息
消息模型如下图
![image](https://user-images.githubusercontent.com/11751256/188567061-79b5f886-eb6e-4577-8a8d-c2a176cbcb02.png)

运行示例代码：
```shell script
cd example
# 运行发送方示例，使用 ctrl+C 退出
make run_dup_sender
# 运行接受者示例，使用 ctrl+C 退出
make run_dup_receiver
```

## 广播消息

### Fan-out 模式

参见 `example/fan-out` 示例
