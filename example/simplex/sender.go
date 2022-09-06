package main

import (
	"os"
	"os/signal"
	"syscall"

	"github.com/aluka-7/amq"
	_ "github.com/aluka-7/amq-rabbit"
	"github.com/aluka-7/amq/message"
	"github.com/aluka-7/amq/node"
	"github.com/aluka-7/configuration"
	"github.com/aluka-7/configuration/backends"
	"github.com/rs/zerolog/log"
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
		msg := message.NewSimplexMessage(message.NewMsgId().Id())
		msg.SetType("test-simplex")
		msg.Destination = client.BuildQueueName("8888")
		msg.Source = client.BuildQueueName("9999")
		msg.SetBody(message.NewMessageBody().Add("hello", "world"))
		if err := client.Send(msg); err != nil {
			log.Err(err).Msgf("send simplex-msg has error:%+v", err)
		}
		log.Info().Msg("simplex-msg sender listening...")
		c := make(chan os.Signal, 1)
		signal.Notify(c, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT)
		for {
			s := <-c
			log.Info().Msgf("simplex-msg sender receive a signal: %s", s.String())
			switch s {
			case syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT:
				log.Info().Msg("simplex-msg sender exit")
				return
			default:
				return
			}
		}
	} else {
		log.Err(err).Msgf("init client has error:%+v", err)
	}
}

type senderProcessor struct {
}

func (s senderProcessor) GetType() string {
	return "test-simplex"
}

func (s senderProcessor) OnReceived(msg interface{}) (*message.MsgBody, error) {
	return nil, nil
}

func (s senderProcessor) OnRecipientAckReceived(msgId string, rsp *message.MsgBody) (*message.MsgBody, error) {
	log.Info().Msgf("收到测试单向事务回复(msgId:%s)信息:%+v\n", msgId, rsp)
	return nil, nil
}

func (s senderProcessor) OnSenderAckReceived(msgId string, rsp *message.MsgBody) error {
	return nil
}
