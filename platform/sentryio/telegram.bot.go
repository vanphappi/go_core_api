package sentryio

import (
	"log"

	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api"
)

type TelegramBot struct {
	BotToken  string
	ChannelID int64
	Bot       *tgbotapi.BotAPI
}

// NewTelegramBot creates a new instance of TelegramBot.
func (tb *TelegramBot) NewTelegramBot() (*TelegramBot, error) {

	bot, err := tgbotapi.NewBotAPI(tb.BotToken)

	if err != nil {
		return nil, err
	}

	return &TelegramBot{Bot: bot, ChannelID: tb.ChannelID}, nil
}

// SendMessage sends a message to a Telegram chat.
func (tb *TelegramBot) SendMessage(message string) error {
	msg := tgbotapi.NewMessage(tb.ChannelID, message)

	_, err := tb.Bot.Send(msg)

	if err != nil {
		log.Fatal(err)
	}

	return err
}

// SendMessage sends a message to a Telegram chat.
func (tb *TelegramBot) Listen() {
	for {
		select {
		case err, ok := <-SentryNotification:
			if !ok {
				// Channel closed, exit loop
				return
			}
			// Forward error message to Telegram
			if err.Message != nil {
				if sendErr := tb.SendMessage(err.String()); sendErr != nil {
					log.Fatal("Failed to send message via Telegram:", sendErr)
				}
			}
		default:
			continue
		}
	}
}
