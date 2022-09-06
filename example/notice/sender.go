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
		msg := message.NewNoticeMessage(message.NewMsgId().Id())
		msg.SetType("test-notice")
		msg.SetBody(message.NewMessageBody().Add("hello", "world"))
		msg.Destination = client.BuildQueueName("8888")
		if err := client.Send(msg); err != nil {
			log.Err(err).Msgf("send notice-msg has error:%+v", err)
		}
		log.Info().Msg("notice-msg sender listening...")
		c := make(chan os.Signal, 1)
		signal.Notify(c, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT)
		for {
			s := <-c
			log.Info().Msgf("notice-msg sender receive a signal: %s", s.String())
			switch s {
			case syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT:
				log.Info().Msg("notice-msg sender exit")
				return
			default:
				return
			}
		}
	} else {
		log.Err(err).Msgf("init client has error:%+v", err)
	}
}
