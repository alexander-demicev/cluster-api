/*
Copyright 2025 The Kubernetes Authors.

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

package inplaceupdate_test

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	runtimehooksv1 "sigs.k8s.io/cluster-api/api/runtime/hooks/v1alpha1"
	"sigs.k8s.io/cluster-api/test/extension/handlers/inplaceupdate"
)

func TestCanUpdateMachine(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = clusterv1.AddToScheme(scheme)

	client := fake.NewClientBuilder().WithScheme(scheme).Build()
	handlers := inplaceupdate.NewExtensionHandlers(client)

	tests := []struct {
		name             string
		changes          []string
		expectedAccepted []string
		expectSuccess    bool
	}{
		{
			name:             "kubernetes version update",
			changes:          []string{"machine.spec.version"},
			expectedAccepted: []string{"machine.spec.version"},
			expectSuccess:    true,
		},
		{
			name:             "memory update",
			changes:          []string{"infraMachine.spec.memoryMiB"},
			expectedAccepted: []string{"infraMachine.spec.memoryMiB"},
			expectSuccess:    true,
		},
		{
			name:             "multiple supported changes",
			changes:          []string{"machine.spec.version", "infraMachine.spec.memoryMiB"},
			expectedAccepted: []string{"machine.spec.version", "infraMachine.spec.memoryMiB"},
			expectSuccess:    true,
		},
		{
			name:             "unsupported change",
			changes:          []string{"machine.spec.unsupportedField"},
			expectedAccepted: []string{},
			expectSuccess:    true, // Success but no accepted changes
		},
		{
			name:             "mixed supported and unsupported",
			changes:          []string{"machine.spec.version", "machine.spec.unsupportedField"},
			expectedAccepted: []string{"machine.spec.version"},
			expectSuccess:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			request := &runtimehooksv1.CanUpdateMachineRequest{
				Changes: tt.changes,
			}
			response := &runtimehooksv1.CanUpdateMachineResponse{}

			handlers.DoCanUpdateMachine(ctx, request, response)

			if tt.expectSuccess {
				assert.Equal(t, runtimehooksv1.ResponseStatusSuccess, response.Status)
			} else {
				assert.Equal(t, runtimehooksv1.ResponseStatusFailure, response.Status)
			}

			// Parse accepted changes from message
			var acceptedChanges []string
			if message := response.Message; message != "" {
				acceptedChanges = strings.Split(message, ",")
			}
			assert.ElementsMatch(t, tt.expectedAccepted, acceptedChanges)
		})
	}
}
