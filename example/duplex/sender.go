package main

import (
	"github.com/aluka-7/amq"
	_ "github.com/aluka-7/amq-rabbit"
	"github.com/aluka-7/amq/message"
	"github.com/aluka-7/amq/node"
	"github.com/aluka-7/configuration"
	"github.com/aluka-7/configuration/backends"
	"github.com/aluka-7/utils"
	"github.com/rs/zerolog/log"
	"time"
)

func main() {
	conf := configuration.MockEngine(nil, backends.StoreConfig{Exp: map[string]string{
		"/system/base/amq/biz": "{\"provider\":\"Rabbit\",\"parameter\":{\"username\":\"guest\",\"password\":\"guest\",\"brokerURL\":\"localhost:5672\"},\"partitions\":1}",
	}})

	if client, err := amq.Engine(conf, "9999").Client(node.BIZ); err == nil {
		client.AddProcessor(&senderProcessor{})
		if cls, err := client.Start([]int{}); err == nil {
			defer cls()
		}
		log.Info().Msg("duplex-msg sender listening...")
		i := 1
		for {
			msg := message.NewDuplexMessage(message.NewMsgId().Id())
			msg.SetType("test-duplex")
			msg.Source = client.BuildQueueName("9999")
			msg.DestinationAck = client.BuildQueueName("8888")
			msg.DestinationNew = client.BuildQueueName("8888")
			msg.SetBody(message.NewMessageBody().Add("hello", "world").Add("index", utils.ToStr(i)))
			if err := client.Send(msg); err != nil {
				log.Err(err).Msgf("send duplex-msg has error:%+v", err)
			}
			i++
			time.Sleep(time.Second * 10)
		}
	} else {
		log.Err(err).Msgf("init client has error:%+v", err)
	}
}

type senderProcessor struct {
}

func (s senderProcessor) GetType() string {
	return "test-duplex"
}

func (s senderProcessor) OnReceived(msg interface{}) (*message.MsgBody, error) {
	log.Info().Msgf("OnReceived收到测试双向事务信息：%+v", msg)
	return nil, nil
}

func (s senderProcessor) OnRecipientAckReceived(msgId string, rsp *message.MsgBody) (*message.MsgBody, error) {
	log.Info().Msgf("OnRecipientAckReceived收到测试双向事务回复(msgId:%s)信息:%+v\n", msgId, rsp)
	rsp.Add("hello", "duplex")
	return rsp, nil
}

func (s senderProcessor) OnSenderAckReceived(msgId string, rsp *message.MsgBody) error {
	return nil
}
