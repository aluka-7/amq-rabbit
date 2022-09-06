package rabbitmq

import (
	"encoding/json"
	"errors"

	"github.com/aluka-7/rabbitmq"
	"github.com/rs/zerolog/log"
	"github.com/streadway/amqp"

	"github.com/aluka-7/amq"
	"github.com/aluka-7/amq/message"
	"github.com/aluka-7/amq/node"
	"github.com/aluka-7/amq/provider"
)

func init() {
	provider.Register("Rabbit", &rabbitProvider{})
}

type rabbitProvider struct {
	client *rabbitmq.Client
	node   node.Node
}

// 初始化消息中间件
func (rp *rabbitProvider) New(node node.Node, cfg map[string]string) provider.Provider {
	rp.node = node
	client, err := rabbitmq.New(cfg["brokerURL"], cfg["username"], cfg["password"], cfg["vhost"]).Open()
	if err != nil {
		panic("not connected to broker")
	}
	rp.client = client
	return rp
}

/**
 * 监听指定的消息队列，当有新消息或事务应答消息送达时会调用监听器接口，可同时监听多个不同的队列。特别注意：同一个消息队列只能有一个监听器！
 *
 * @param name     要监听的消息队列
 * @param listener 消息监听器
 * @throws error
 */
func (rp *rabbitProvider) Listen(name string, listener provider.MessageListener) (func(), error) {
	rp.Cancel(name)
	consumer, err := rp.client.Consumer(name)
	if err != nil {
		log.Err(err).Msgf("Create consumer failed, %v\n", err)
		return nil, errors.New("Create consumer failed ")
	}
	exchange, queue, route := DestroyQueueName(name)
	msgC := make(chan rabbitmq.Delivery, 1)
	consumer.SetMsgCallback(msgC).SetQos(1).SetExchangeBinds([]*rabbitmq.ExchangeBinds{
		{
			Exch: rabbitmq.DefaultExchange(exchange, amqp.ExchangeDirect, nil),
			Bindings: []*rabbitmq.Binding{
				{RouteKey: route, Queues: []*rabbitmq.Queue{rabbitmq.DefaultQueue(queue, nil)}},
			},
		},
	})
	if err = consumer.Open(); err != nil {
		log.Err(err).Msg("Open failed")
		return nil, errors.New("Open failed ")
	}
	go rp.recoverMask(msgC, listener)
	return func() {
		log.Info().Msg("Close msg callback channel")
		close(msgC)
	}, nil
}

func (rp *rabbitProvider) recoverMask(msgC chan rabbitmq.Delivery, listener provider.MessageListener) {
	defer func() {
		if err := recover(); err != nil {
			log.Error().Msgf("[RabbitMQ-%s] goroutine handle panic:%+v", rp.node.String(), err)
			go rp.recoverMask(msgC, listener)
		}
	}()
	for {
		select {
		case msg := <-msgC:
			rp.maskMsg(msg, listener)
		}
	}
}
func (rp *rabbitProvider) maskMsg(msg rabbitmq.Delivery, listener provider.MessageListener) {
	payload := &message.MsgPayload{}
	defer msg.Ack(false)
	var err error
	if err = json.Unmarshal(msg.Body, payload); err != nil {
		log.Error().Msgf("[RabbitMQ-%s] Decode msg failed: %s", rp.node.String(), err)
		return
	}
	log.Debug().Msgf("[RabbitMQ-%s]收到消息 %+v", rp.node.String(), payload)
	// check signature
	if message.Signature(payload) != payload.Sign {
		log.Warn().Msgf("[AMQClient-%s]收到无效签名消息 %s", rp.node.String(), payload.MsgId)
		return
	}
	var retMsg *message.MsgPayload
	if payload.Phase == message.SenderReq {
		retMsg, err = amq.HandleNew(payload, listener)
	} else {
		retMsg, err = amq.HandleAck(payload, listener)
	}
	if err != nil {
		log.Err(err).Msgf("Handle msg error: %s", err)
		return
	}
	if retMsg != nil {
		if err = rp.sendMessage(retMsg); err != nil {
			log.Err(err).Msgf("[RabbitMQ-%s]发送消息到AMQ发生错误:%+v", rp.node.String(), err)
		}
	}
}

/**
 * 取消对指定队列的监听。
 *
 * @param name
 */
func (rp *rabbitProvider) Cancel(name string) {
	_ = rp.client.CloseConsumer(name)
}

/**
 * 发送AMQ消息，可根据业务需求发送三类消息：
 * <ul>
 * <li>{@link NoticeMessage}：通知类消息，只保证消息被成功发送到AMQ中。</li>
 * <li>{@link SimplexMessage}：单向事务消息，通过收到接收方的确认消息来完成事务。</li>
 * <li>{@link DuplexMessage}：双向事务消息，通过接收方和发送方各分别确认来完对应的事务。</li>
 * </ul>
 *
 * @param message
 * @throws error
 */
func (rp *rabbitProvider) Send(msg interface{}) error {
	var mpl *message.MsgPayload
	switch msg.(type) {
	case *message.NoticeMessage:
		mpl = message.NoticePayload(msg.(*message.NoticeMessage))
	case *message.SimplexMessage:
		mpl = message.SimplexPayload(msg.(*message.SimplexMessage))
	case *message.DuplexMessage:
		mpl = message.DuplexPayload(msg.(*message.DuplexMessage))
	default:
		mpl = &message.MsgPayload{}
	}
	return rp.sendMessage(mpl)
}
func (rp *rabbitProvider) sendMessage(payload *message.MsgPayload) error {
	if name, err := payload.SendQueueName(); err == nil {
		log.Debug().Msgf("[RabbitMQ-%s]发送消息到AMQ(%s):%s", rp.node.String(), name, payload)
		if p, err := rp.client.Producer(name); err == nil {
			exchange, queue, route := DestroyQueueName(name)
			return p.ForDirect(exchange, route, queue, payload.String())
		} else {
			log.Err(err).Msgf("Create producer failed, %v\n", err)
			return errors.New("Create producer failed ")
		}
	} else {
		return err
	}
}

/**
 * 关闭到消息中间件的连接，清除资源。
 */
func (rp *rabbitProvider) Close() {
	rp.client.Close()
}

/**
 * 使用当前客户端分解一个AMQ消息的队列名称，{name}名称满足格式：sys_amq_{systemId}_{node}或者sys_amq_{systemId}_{node}_p{partition}
 * 其中{systemId}为目标系统的四位数数字ID，{node}为目标系统监听的AMQ节点标示(参考{@link AMQNode}。
 *
 * @param name AMQ消息的队列名称
 * @return "sys_amq","{systemId}",["{node}","p{partition}"]
 */
func DestroyQueueName(name string) (string, string, string) {
	return "sys_amq", name[8:12], name[8:]
}
