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

package inplaceupdate

import (
	"context"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	runtimehooksv1 "sigs.k8s.io/cluster-api/exp/runtime/hooks/api/v1alpha1"
)

// InPlaceUpdateHandler handles lifecycle hooks for in-place updates.
type InPlaceUpdateHandler struct {
	client client.Client
}

// NewInPlaceUpdateHandler returns an InPlaceUpdateHandler instance.
func NewInPlaceUpdateHandler(client client.Client) *InPlaceUpdateHandler {
	return &InPlaceUpdateHandler{
		client: client,
	}
}

// DoInPlaceUpdate implements the HandlerFunc for the InPlaceUpdate hook.
func (iuh *InPlaceUpdateHandler) DoInPlaceUpdate(ctx context.Context, request *runtimehooksv1.InPlaceUpdateRequest, response *runtimehooksv1.InPlaceUpdateResponse) {
	log := ctrl.LoggerFrom(ctx)
	log.Info("InPlaceUpdate is called")

	// TODO: Implement the test logic here.
	response.SetMessage("InPlaceUpdate is called")
	response.SetStatus(runtimehooksv1.ResponseStatusSuccess)
}
