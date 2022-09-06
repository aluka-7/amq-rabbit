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
	if client, err := amq.Engine(conf, "8888").Client(node.BIZ); err == nil {
		client.AddProcessor(&receiverProcessor{})
		if cls, err := client.Start([]int{}); err == nil {
			defer cls()
		}
		log.Info().Msg("duplex-msg receiver listening...")
		c := make(chan os.Signal, 1)
		signal.Notify(c, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT)
		for {
			s := <-c
			log.Info().Msgf("duplex-msg receiver receive a signal: %s", s.String())
			switch s {
			case syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT:
				log.Info().Msg("duplex-msg receiver exit")
				return
			default:
				return
			}
		}
	}
}

type receiverProcessor struct {
}

func (r receiverProcessor) GetType() string {
	return "test-duplex"
}

func (r receiverProcessor) OnReceived(msg interface{}) (*message.MsgBody, error) {
	log.Info().Msgf("OnReceived收到测试双向事务信息：%+v", msg)
	mb := msg.(*message.DuplexMessage).Body
	mb.Add("hi", "duplex")
	return mb, nil
}

func (r receiverProcessor) OnRecipientAckReceived(msgId string, rsp *message.MsgBody) (*message.MsgBody, error) {
	return nil, nil
}

func (r receiverProcessor) OnSenderAckReceived(msgId string, rsp *message.MsgBody) error {
	log.Info().Msgf("OnSenderAckReceived收到测试双向事务回复(msgId:%s)信息:%+v\n", msgId, rsp)
	return nil
}
