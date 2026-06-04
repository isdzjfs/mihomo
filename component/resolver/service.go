package resolver

import (
	"context"

	D "github.com/miekg/dns"
)

var DefaultService Service

func SetDefaultService(service Service) {
	stateMu.Lock()
	DefaultService = service
	stateMu.Unlock()
}

func DefaultServiceValue() Service {
	stateMu.RLock()
	defer stateMu.RUnlock()
	return DefaultService
}

type Service interface {
	ServeMsg(ctx context.Context, msg *D.Msg) (*D.Msg, error)
}

// ServeMsg with a dns.Msg, return resolve dns.Msg
func ServeMsg(ctx context.Context, msg *D.Msg) (*D.Msg, error) {
	if server := DefaultServiceValue(); server != nil {
		return server.ServeMsg(ctx, msg)
	}

	return nil, ErrIPNotFound
}
