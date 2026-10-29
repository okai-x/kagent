/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package remotemcpserver

import (
	"context"
	"errors"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apiMeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/kagent-dev/kagent/go/api/v1alpha3"
	"github.com/kagent-dev/kagent/go/core/internal/database"
	toolservice "github.com/kagent-dev/kagent/go/core/internal/service/tool"
	"github.com/kagent-dev/kagent/go/core/pkg/consts"
)

type fakeDiscoverer struct {
	tools []toolservice.MCPAppTool
	err   error
	ref   toolservice.MCPServerRef
	calls int
}

type fakeCatalog struct {
	writes  int
	server  *database.ToolServer
	tools   []*v1alpha3.MCPTool
	deleted string
}

func (f *fakeCatalog) RefreshToolServer(_ context.Context, server *database.ToolServer, tools ...*v1alpha3.MCPTool) error {
	f.writes++
	f.server = server
	f.tools = tools
	return nil
}

func (f *fakeCatalog) DeleteToolServer(_ context.Context, name, groupKind string) error {
	f.deleted = name + "|" + groupKind
	return nil
}

func (f *fakeDiscoverer) ListTools(_ context.Context, ref toolservice.MCPServerRef) ([]toolservice.MCPAppTool, error) {
	f.ref = ref
	f.calls++
	return f.tools, f.err
}

func TestReconcilePublishesSortedDiscovery(t *testing.T) {
	server := testServer()
	kube := testClient(t, server)
	discoverer := &fakeDiscoverer{tools: []toolservice.MCPAppTool{
		{Name: "zeta", Description: "last"},
		{Name: "alpha", Description: "first"},
	}}
	catalog := &fakeCatalog{}
	reconciler := New(kube, discoverer, catalog, BuiltinCatalog{})

	result, err := reconciler.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(server)})
	if err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if result.RequeueAfter != 5*time.Minute {
		t.Fatalf("Reconcile() requeue = %s, want 5m", result.RequeueAfter)
	}
	if discoverer.ref.Ref != client.ObjectKeyFromObject(server) || discoverer.ref.GroupKind != "RemoteMCPServer.api.kagent.dev" {
		t.Fatalf("discovery ref = %#v", discoverer.ref)
	}

	updated := getServer(t, kube, server)
	if updated.Status.ObservedGeneration != server.Generation {
		t.Fatalf("observed generation = %d, want %d", updated.Status.ObservedGeneration, server.Generation)
	}
	if len(updated.Status.DiscoveredTools) != 2 || updated.Status.DiscoveredTools[0].Name != "alpha" || updated.Status.DiscoveredTools[1].Name != "zeta" {
		t.Fatalf("discovered tools = %#v", updated.Status.DiscoveredTools)
	}
	condition := apiMeta.FindStatusCondition(updated.Status.Conditions, conditionAccepted)
	if condition == nil || condition.Status != metav1.ConditionTrue || condition.Reason != "DiscoverySucceeded" {
		t.Fatalf("Accepted condition = %#v", condition)
	}
	if catalog.server == nil || catalog.server.Name != "test/tools" || catalog.server.GroupKind != remoteGroupKind || catalog.server.LastConnected == nil {
		t.Fatalf("catalog server = %#v", catalog.server)
	}
	if len(catalog.tools) != 2 || catalog.tools[0].Name != "alpha" || catalog.tools[1].Name != "zeta" {
		t.Fatalf("catalog tools = %#v", catalog.tools)
	}
	discoverer.tools[0], discoverer.tools[1] = discoverer.tools[1], discoverer.tools[0]
	_, err = reconciler.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(server)})
	if err != nil || discoverer.calls != 2 || catalog.writes != 1 {
		t.Fatalf("unchanged discovery: error = %v, discovery calls = %d, catalog writes = %d", err, discoverer.calls, catalog.writes)
	}
}

func TestReconcilePublishesFailureAndClearsStaleTools(t *testing.T) {
	server := testServer()
	server.Status.DiscoveredTools = []*v1alpha3.MCPTool{{Name: "stale", Description: "stale"}}
	kube := testClient(t, server)
	discoverer := &fakeDiscoverer{err: errors.New("upstream unavailable")}
	catalog := &fakeCatalog{}

	_, err := New(kube, discoverer, catalog, BuiltinCatalog{}).Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(server)})
	if err == nil {
		t.Fatal("Reconcile() error = nil, want discovery failure")
	}
	updated := getServer(t, kube, server)
	if updated.Status.ObservedGeneration != server.Generation || len(updated.Status.DiscoveredTools) != 0 {
		t.Fatalf("failed discovery status = %#v", updated.Status)
	}
	condition := apiMeta.FindStatusCondition(updated.Status.Conditions, conditionAccepted)
	if condition == nil || condition.Status != metav1.ConditionFalse || condition.Reason != "DiscoveryFailed" || condition.Message != "upstream unavailable" {
		t.Fatalf("Accepted condition = %#v", condition)
	}
	if catalog.server == nil || catalog.server.LastConnected != nil || len(catalog.tools) != 0 {
		t.Fatalf("failed discovery catalog = server %#v, tools %#v", catalog.server, catalog.tools)
	}
}

func TestReconcileAcceptsWithoutDiscoveryWhenDisabled(t *testing.T) {
	server := testServer()
	server.Labels = map[string]string{consts.DiscoveryLabel: consts.DiscoveryDisabled}
	server.Status.DiscoveredTools = []*v1alpha3.MCPTool{{Name: "stale", Description: "stale"}}
	kube := testClient(t, server)
	discoverer := &fakeDiscoverer{err: errors.New("the controller must not dial an opted-out server")}
	catalog := &fakeCatalog{}

	result, err := New(kube, discoverer, catalog, BuiltinCatalog{}).Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(server)})
	if err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if result.RequeueAfter != 5*time.Minute {
		t.Fatalf("Reconcile() requeue = %s, want 5m", result.RequeueAfter)
	}
	if discoverer.calls != 0 {
		t.Fatalf("discovery calls = %d, want 0", discoverer.calls)
	}

	updated := getServer(t, kube, server)
	if updated.Status.ObservedGeneration != server.Generation || len(updated.Status.DiscoveredTools) != 0 {
		t.Fatalf("disabled discovery status = %#v", updated.Status)
	}
	condition := apiMeta.FindStatusCondition(updated.Status.Conditions, conditionAccepted)
	if condition == nil || condition.Status != metav1.ConditionTrue || condition.Reason != "DiscoveryDisabled" || condition.Message != "Global tool discovery is disabled by the kagent.dev/discovery label; agents resolve the tool list at run time" {
		t.Fatalf("Accepted condition = %#v", condition)
	}
	if catalog.server == nil || catalog.server.Name != "test/tools" || catalog.server.LastConnected != nil || len(catalog.tools) != 0 {
		t.Fatalf("disabled discovery catalog = server %#v, tools %#v", catalog.server, catalog.tools)
	}
}

func TestReconcileDeletesCatalogProjection(t *testing.T) {
	catalog := &fakeCatalog{}
	request := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "test", Name: "gone"}}

	if _, err := New(testClient(t), &fakeDiscoverer{}, catalog, BuiltinCatalog{}).Reconcile(t.Context(), request); err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	want := "test/gone|" + remoteGroupKind
	if catalog.deleted != want {
		t.Fatalf("catalog delete = %q, want %q", catalog.deleted, want)
	}
}

func TestReferencesDependency(t *testing.T) {
	server := testServer()
	server.Spec.HeadersFrom = []v1alpha3.ValueRef{
		{Name: "Authorization", ValueFrom: &v1alpha3.ValueSource{Type: v1alpha3.SecretValueSource, Name: "auth", Key: "token"}},
		{Name: "X-Config", ValueFrom: &v1alpha3.ValueSource{Type: v1alpha3.ConfigMapValueSource, Name: "headers", Key: "value"}},
	}

	tests := []struct {
		name string
		obj  client.Object
		want bool
	}{
		{name: "header secret", obj: &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "auth"}}, want: true},
		{name: "header ConfigMap", obj: &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "headers"}}, want: true},
		{name: "unrelated secret", obj: &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "other"}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := referencesDependency(server, test.obj); got != test.want {
				t.Fatalf("referencesDependency() = %t, want %t", got, test.want)
			}
		})
	}
}

func testServer() *v1alpha3.RemoteMCPServer {
	return &v1alpha3.RemoteMCPServer{
		ObjectMeta: metav1.ObjectMeta{Namespace: "test", Name: "tools", Generation: 3},
		Spec: v1alpha3.RemoteMCPServerSpec{
			Description: "tools", Protocol: v1alpha3.RemoteMCPServerProtocolStreamableHttp,
			URL: "https://tools.example/mcp",
		},
	}
}

func testClient(t *testing.T, objects ...client.Object) client.Client {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := v1alpha3.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	return fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&v1alpha3.RemoteMCPServer{}).
		WithObjects(objects...).Build()
}

func getServer(t *testing.T, kube client.Client, source *v1alpha3.RemoteMCPServer) *v1alpha3.RemoteMCPServer {
	t.Helper()
	result := &v1alpha3.RemoteMCPServer{}
	key := types.NamespacedName{Namespace: source.Namespace, Name: source.Name}
	if err := kube.Get(t.Context(), key, result); err != nil {
		t.Fatal(err)
	}
	return result
}

func TestActorJWTDisablesGlobalDiscovery(t *testing.T) {
	server := testServer()
	server.Spec.ActorJWT = &v1alpha3.ActorJWT{Audiences: []string{"mcp"}}
	server.Status.DiscoveredTools = []*v1alpha3.MCPTool{{Name: "stale", Description: "stale"}}
	kube := testClient(t, server)
	discoverer := &fakeDiscoverer{err: errors.New("controller must not contact an Actor-authenticated server")}
	catalog := &fakeCatalog{}
	_, err := New(kube, discoverer, catalog, BuiltinCatalog{}).Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(server)})
	if err != nil {
		t.Fatal(err)
	}
	if discoverer.calls != 0 {
		t.Fatal("unexpected global discovery")
	}
	updated := getServer(t, kube, server)
	condition := apiMeta.FindStatusCondition(updated.Status.Conditions, conditionAccepted)
	if condition == nil || condition.Status != metav1.ConditionTrue || condition.Reason != "DiscoveryDisabled" || condition.Message != "Global tool discovery requires an executing Actor because actorJwt is enabled; agents resolve the tool list at run time" {
		t.Fatalf("Accepted condition = %#v", condition)
	}
	if len(updated.Status.DiscoveredTools) != 0 {
		t.Fatal("stale discovery status must be cleared")
	}
	if len(catalog.tools) != 0 {
		t.Fatal("stale tools must be cleared")
	}
}

func TestBuiltinCatalogRequiresExactEndpointAndProtocol(t *testing.T) {
	const endpoint = "http://kagent-controller.kagent.svc.cluster.local:8083/mcp"
	for _, test := range []struct {
		name     string
		endpoint string
		protocol v1alpha3.RemoteMCPServerProtocol
		local    bool
	}{
		{"builtin", endpoint, v1alpha3.RemoteMCPServerProtocolStreamableHttp, true},
		{"other origin", "http://other-controller.kagent.svc.cluster.local:8083/mcp", v1alpha3.RemoteMCPServerProtocolStreamableHttp, false},
		{"other port", "http://kagent-controller.kagent.svc.cluster.local:8084/mcp", v1alpha3.RemoteMCPServerProtocolStreamableHttp, false},
		{"other scheme", "https://kagent-controller.kagent.svc.cluster.local:8083/mcp", v1alpha3.RemoteMCPServerProtocolStreamableHttp, false},
		{"other path", endpoint + "/other", v1alpha3.RemoteMCPServerProtocolStreamableHttp, false},
		{"query", endpoint + "?tenant=other", v1alpha3.RemoteMCPServerProtocolStreamableHttp, false},
		{"sse", endpoint, v1alpha3.RemoteMCPServerProtocolSse, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := testServer()
			server.Spec.URL = test.endpoint
			server.Spec.Protocol = test.protocol
			server.Spec.HeadersFrom = []v1alpha3.ValueRef{{Name: "Authorization", Value: "Bearer kagent-credential-injected"}}
			kube := testClient(t, server)
			discoverer := &fakeDiscoverer{err: errors.New("401 Unauthorized")}
			catalog := &fakeCatalog{}
			reconciler := New(kube, discoverer, catalog, BuiltinCatalog{
				Endpoint: endpoint, Tools: []toolservice.MCPAppTool{{Name: "list_sessions", Description: "registered tool"}},
			})
			_, err := reconciler.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(server)})
			updated := getServer(t, kube, server)
			condition := apiMeta.FindStatusCondition(updated.Status.Conditions, conditionAccepted)
			if !test.local {
				if err == nil || discoverer.calls != 1 || condition == nil || condition.Status != metav1.ConditionFalse {
					t.Fatalf("remote discovery: error = %v, calls = %d, condition = %#v", err, discoverer.calls, condition)
				}
				return
			}
			if err != nil || discoverer.calls != 0 {
				t.Fatalf("builtin discovery: error = %v, remote calls = %d", err, discoverer.calls)
			}
			if condition == nil || condition.Status != metav1.ConditionTrue || condition.ObservedGeneration != server.Generation || updated.Status.ObservedGeneration != server.Generation {
				t.Fatalf("builtin discovery status = %#v", updated.Status)
			}
			if len(catalog.tools) != 1 || catalog.tools[0].Name != "list_sessions" || catalog.tools[0].Description != "registered tool" || catalog.server.LastConnected == nil {
				t.Fatalf("builtin catalog = %#v", catalog)
			}
		})
	}
}

func TestBuiltinCatalogPreservesDiscoveryOptOut(t *testing.T) {
	for _, actorJWT := range []bool{false, true} {
		server := testServer()
		if actorJWT {
			server.Spec.ActorJWT = &v1alpha3.ActorJWT{Audiences: []string{"mcp"}}
		} else {
			server.Labels = map[string]string{consts.DiscoveryLabel: consts.DiscoveryDisabled}
		}
		kube := testClient(t, server)
		discoverer := &fakeDiscoverer{err: errors.New("must not contact disabled server")}
		catalog := &fakeCatalog{}
		reconciler := New(kube, discoverer, catalog, BuiltinCatalog{
			Endpoint: server.Spec.URL, Tools: []toolservice.MCPAppTool{{Name: "list_sessions"}},
		})
		_, err := reconciler.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(server)})
		if err != nil || discoverer.calls != 0 || len(catalog.tools) != 0 {
			t.Fatalf("disabled builtin: error = %v, remote calls = %d, catalog = %#v", err, discoverer.calls, catalog)
		}
		condition := apiMeta.FindStatusCondition(getServer(t, kube, server).Status.Conditions, conditionAccepted)
		if condition == nil || condition.Reason != "DiscoveryDisabled" {
			t.Fatalf("disabled builtin condition = %#v", condition)
		}
	}
}
