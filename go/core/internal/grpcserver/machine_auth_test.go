package grpcserver

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	authenticationv1 "k8s.io/api/authentication/v1"
	"k8s.io/apimachinery/pkg/runtime"
	kubefake "k8s.io/client-go/kubernetes/fake"
	kubetesting "k8s.io/client-go/testing"

	apiv1 "github.com/kagent-dev/kagent/go/api/gen/kagent/api/v1"
	authimpl "github.com/kagent-dev/kagent/go/core/internal/httpserver/auth"
	"github.com/kagent-dev/kagent/go/core/internal/jwttest"
	"github.com/kagent-dev/kagent/go/core/internal/runtimeauth"
	"github.com/kagent-dev/kagent/go/core/internal/substrate"
	"github.com/kagent-dev/kagent/go/core/pkg/auth"
)

func TestServiceAccountOnlyNativeGRPC(t *testing.T) {
	issuer := jwttest.New(t)
	verifier, err := substrate.NewActorJWTVerifier(t.Context(), issuer.URL, issuer.CAFile, substrate.RuntimeJWTAudience)
	require.NoError(t, err)
	store := &memoryRPCStore{session: &apiv1.Session{Id: uuid.NewString(), Creator: "owner",
		Agent: &apiv1.ResourceReference{Namespace: "team-a", Name: "assistant"}}}
	store.session.A2AAuthority = substrate.ActorHost("team-a", substrate.ActorName(store.session.Id), "")
	sign := func(subject string) string {
		t.Helper()
		token, err := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
			"sub": subject, "exp": time.Now().Add(time.Hour).Unix(),
		}).SignedString([]byte("untrusted-test-signature"))
		require.NoError(t, err)
		return token
	}
	valid, rejected := sign("system:serviceaccount:kiln:backend"), sign("system:serviceaccount:kiln:rejected")
	kube := kubefake.NewClientset()
	kube.PrependReactor("create", "tokenreviews", func(action kubetesting.Action) (bool, runtime.Object, error) {
		request := action.(kubetesting.CreateAction).GetObject().(*authenticationv1.TokenReview)
		require.Equal(t, []string{"kagent-invocation"}, request.Spec.Audiences)
		return true, &authenticationv1.TokenReview{Status: authenticationv1.TokenReviewStatus{
			Authenticated: request.Spec.Token == valid, Audiences: request.Spec.Audiences,
			User: authenticationv1.UserInfo{Username: "system:serviceaccount:kiln:backend", UID: "kiln-uid"},
		}}, nil
	})
	machines := authimpl.NewServiceAccountProvider(&authimpl.RejectUserAuthenticator{}, kube.AuthenticationV1().TokenReviews(), "cluster-a")
	provider := runtimeauth.NewProvider(machines, verifier, store, issuer.URL)
	connection := newConfigurationConnection(t, nil, func(config *Config) { config.Authenticator = provider })
	client := apiv1.NewSystemServiceClient(connection)
	actor := issuer.Token(t, issuer.Claims("team-a", substrate.ActorName(store.session.Id), "actor-uid"))
	for _, test := range []struct {
		name, token, id string
	}{
		{name: "no credential"},
		{name: "opaque credential", token: "opaque"},
		{name: "unverified user JWT", token: sign("admin")},
		{name: "rejected service account", token: rejected},
		{name: "verified service account", token: valid, id: auth.PrincipalID("kubernetes:cluster-a", "kiln-uid")},
		{name: "verified Actor", token: actor, id: auth.PrincipalID("substrate-actor", "team-a/actor-uid")},
	} {
		t.Run(test.name, func(t *testing.T) {
			md := metadata.Pairs("x-user-id", "forged-owner", "x-agent-name", "forged/agent")
			if test.token != "" {
				md.Set("authorization", "Bearer "+test.token)
			}
			result, err := client.GetCurrentUser(metadata.NewOutgoingContext(t.Context(), md), &apiv1.GetCurrentUserRequest{})
			if test.id == "" {
				require.Equal(t, codes.Unauthenticated, status.Code(err))
				return
			}
			require.NoError(t, err)
			if test.token == valid {
				require.Equal(t, "system:serviceaccount:kiln:backend", result.Claims.Fields["username"].GetStringValue())
			} else {
				require.Equal(t, test.id, result.Claims.Fields["sub"].GetStringValue())
			}
		})
	}
	identity, err := provider.Authenticate(t.Context(), http.Header{"Authorization": {"Bearer " + valid}}, nil)
	require.NoError(t, err)
	request := httptest.NewRequest(http.MethodPost, "http://private-runtime", nil)
	require.NoError(t, provider.UpstreamAuth(request, identity, ""))
	require.Equal(t, "Bearer "+valid, request.Header.Get("Authorization"))
	require.Equal(t, auth.PrincipalID("kubernetes:cluster-a", "kiln-uid"), request.Header.Get("X-User-Id"))
}
