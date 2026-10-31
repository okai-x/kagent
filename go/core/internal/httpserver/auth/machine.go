package auth

import (
	"context"
	"net/http"
	"net/url"

	"github.com/kagent-dev/kagent/go/core/pkg/auth"
)

// RejectUserAuthenticator disables ordinary user authentication. The enclosing
// ServiceAccountProvider and runtimeauth.Provider still verify machine callers.
type RejectUserAuthenticator struct{}

var _ auth.AuthProvider = (*RejectUserAuthenticator)(nil)

func (*RejectUserAuthenticator) Authenticate(context.Context, http.Header, url.Values) (auth.Session, error) {
	return nil, ErrUnauthenticated
}

func (*RejectUserAuthenticator) UpstreamAuth(request *http.Request, session auth.Session, _ string) error {
	// Product credentials authorize the control plane, never the Actor workload.
	request.Header.Del("Authorization")
	request.Header.Del("X-User-Id")
	if session != nil {
		request.Header.Set("X-User-Id", session.Principal().ID)
	}
	return nil
}
