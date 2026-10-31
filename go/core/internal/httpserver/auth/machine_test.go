package auth_test

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	authenticationv1 "k8s.io/api/authentication/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	kubetesting "k8s.io/client-go/testing"

	authimpl "github.com/kagent-dev/kagent/go/core/internal/httpserver/auth"
	"github.com/kagent-dev/kagent/go/core/pkg/auth"
)

func TestMachineUpstreamAuthStripsServiceAccountCredentials(t *testing.T) {
	token := createTestJWT(map[string]any{"sub": "system:serviceaccount:kiln:backend", "exp": time.Now().Add(time.Hour).Unix()})
	client := fake.NewClientset()
	client.PrependReactor("create", "tokenreviews", func(action kubetesting.Action) (bool, runtime.Object, error) {
		review := action.(kubetesting.CreateAction).GetObject().(*authenticationv1.TokenReview)
		require.Equal(t, token, review.Spec.Token)
		require.Equal(t, []string{"kagent-invocation"}, review.Spec.Audiences)
		return true, &authenticationv1.TokenReview{Status: authenticationv1.TokenReviewStatus{
			Authenticated: true, Audiences: review.Spec.Audiences,
			User: authenticationv1.UserInfo{Username: "system:serviceaccount:kiln:backend", UID: "kiln-uid"},
		}}, nil
	})
	provider := authimpl.NewServiceAccountProvider(&authimpl.RejectUserAuthenticator{}, client.AuthenticationV1().TokenReviews(), "cluster")
	for _, scheme := range []string{"Bearer", "bEaReR"} {
		t.Run(scheme, func(t *testing.T) {
			identity, err := provider.Authenticate(t.Context(), http.Header{
				"Authorization": {scheme + " " + token}, "X-User-Id": {"forged-owner"},
			}, nil)
			require.NoError(t, err)
			request := httptest.NewRequest(http.MethodPost, "http://private-runtime", nil)
			request.Header.Set("Authorization", "Bearer stale-credential")
			request.Header.Set("X-User-Id", "forged-upstream-owner")
			require.NoError(t, provider.UpstreamAuth(request, identity, "session"))
			require.NotContains(t, request.Header, "Authorization")
			require.Equal(t, auth.PrincipalID("kubernetes:cluster", "kiln-uid"), request.Header.Get("X-User-Id"))
		})
	}
	_, err := provider.Authenticate(t.Context(), http.Header{"X-User-Id": {"forged-owner"}}, nil)
	require.ErrorIs(t, err, authimpl.ErrUnauthenticated)
}

func TestMachineUpstreamAuthWithoutBearer(t *testing.T) {
	for _, test := range []struct {
		name    string
		session auth.Session
		userID  string
	}{
		{name: "no identity"},
		{name: "verified identity without bearer", session: auth.NewCredentialSession(auth.Principal{ID: "verified-owner"}, nil), userID: "verified-owner"},
	} {
		t.Run(test.name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodPost, "http://private-runtime", nil)
			request.Header.Set("Authorization", "Bearer stale-credential")
			request.Header.Set("X-User-Id", "forged-owner")
			provider := &authimpl.RejectUserAuthenticator{}
			require.NoError(t, provider.UpstreamAuth(request, test.session, "session"))
			require.NotContains(t, request.Header, "Authorization")
			require.Equal(t, test.userID, request.Header.Get("X-User-Id"))
		})
	}
}
