package grpcserver

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"math/big"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	apiv1 "github.com/kagent-dev/kagent/go/api/gen/kagent/api/v1"
	authimpl "github.com/kagent-dev/kagent/go/core/internal/httpserver/auth"
	"github.com/kagent-dev/kagent/go/core/pkg/auth"
)

func TestAdditionalTLSListenerServesGRPCAndAuthenticatedHTTP(t *testing.T) {
	first, firstCert := servingCertificate(t, 1)
	second, secondCert := servingCertificate(t, 2)
	bundle := filepath.Join(t.TempDir(), "serving.pem")
	require.NoError(t, os.WriteFile(bundle, first, 0o600))
	roots := x509.NewCertPool()
	roots.AddCert(firstCert)
	roots.AddCert(secondCert)
	plain := testTCPListener(t)
	secure := testTCPListener(t)
	provider := &authimpl.RejectUserAuthenticator{}
	mux := http.NewServeMux()
	mux.Handle("/mcp", auth.AuthnMiddleware(provider)(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})))
	server, err := New(Config{
		Listener: plain, TLSBindAddress: secure.Addr().String(), TLSCertFile: bundle, TLSKeyFile: bundle,
		Authenticator: provider, SystemService: testSystemService(), HTTPHandler: mux,
	})
	require.NoError(t, err)
	server.tlsListener = secure
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- server.Start(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			require.NoError(t, err)
		case <-time.After(2 * time.Second):
			t.Error("TLS and plaintext listeners did not stop")
		}
	})

	for _, test := range []struct {
		name, address string
		credentials   credentials.TransportCredentials
	}{
		{"TLS", secure.Addr().String(), credentials.NewTLS(&tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12})},
		{"plaintext", plain.Addr().String(), insecure.NewCredentials()},
	} {
		t.Run(test.name, func(t *testing.T) {
			connection, err := grpc.NewClient(test.address, grpc.WithTransportCredentials(test.credentials))
			require.NoError(t, err)
			defer func() { require.NoError(t, connection.Close()) }()
			client := apiv1.NewSystemServiceClient(connection)
			callContext, callCancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer callCancel()
			_, err = client.GetVersion(callContext, &apiv1.GetVersionRequest{})
			require.NoError(t, err)
			_, err = client.GetCurrentUser(callContext, &apiv1.GetCurrentUserRequest{})
			require.Equal(t, codes.Unauthenticated, status.Code(err))
		})
	}
	transport := &http.Transport{TLSClientConfig: &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: 5 * time.Second}
	for _, origin := range []string{"https://" + secure.Addr().String(), "http://" + plain.Addr().String()} {
		request, err := http.NewRequestWithContext(t.Context(), http.MethodPost, origin+"/mcp", nil)
		require.NoError(t, err)
		response, err := client.Do(request)
		require.NoError(t, err)
		require.NoError(t, response.Body.Close())
		require.Equal(t, http.StatusUnauthorized, response.StatusCode)
	}

	checkCertificate := func(serial int64) {
		t.Helper()
		dialer := tls.Dialer{Config: &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}}
		connection, err := dialer.DialContext(t.Context(), "tcp", secure.Addr().String())
		require.NoError(t, err)
		defer func() { require.NoError(t, connection.Close()) }()
		require.Equal(t, serial, connection.(*tls.Conn).ConnectionState().PeerCertificates[0].SerialNumber.Int64())
	}
	checkCertificate(1)
	require.NoError(t, os.WriteFile(bundle+".next", second, 0o600))
	require.NoError(t, os.Rename(bundle+".next", bundle))
	checkCertificate(2)
	require.NoError(t, os.WriteFile(bundle, []byte("invalid rotated certificate"), 0o600))
	dialer := tls.Dialer{Config: &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}}
	_, err = dialer.DialContext(t.Context(), "tcp", secure.Addr().String())
	require.Error(t, err)
}

func TestTLSBindFailureClosesPlaintextListener(t *testing.T) {
	bundle, _ := servingCertificate(t, 1)
	path := filepath.Join(t.TempDir(), "serving.pem")
	require.NoError(t, os.WriteFile(path, bundle, 0o600))
	plain, occupied := testTCPListener(t), testTCPListener(t)
	server, err := New(Config{
		Listener: plain, TLSBindAddress: occupied.Addr().String(), TLSCertFile: path, TLSKeyFile: path,
		SystemService: testSystemService(),
	})
	require.NoError(t, err)
	require.ErrorContains(t, server.Start(t.Context()), "listen for HTTPS")
	_, err = plain.Accept()
	require.ErrorIs(t, err, net.ErrClosed)
}

func testTCPListener(t *testing.T) net.Listener {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })
	return listener
}

func servingCertificate(t *testing.T, serial int64) ([]byte, *x509.Certificate) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	template := &x509.Certificate{
		SerialNumber: big.NewInt(serial), NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour),
		IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}, DNSNames: []string{"localhost"},
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	require.NoError(t, err)
	privateKey, err := x509.MarshalPKCS8PrivateKey(key)
	require.NoError(t, err)
	bundle := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: privateKey})
	bundle = append(bundle, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})...)
	certificate, err := x509.ParseCertificate(der)
	require.NoError(t, err)
	return bundle, certificate
}
