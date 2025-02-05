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

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clusterv1 "sigs.k8s.io/cluster-api/api/v1beta1"
	runtimecatalog "sigs.k8s.io/cluster-api/exp/runtime/catalog"
)

// InPlaceUpdateRequest is the request for the InPlaceUpdate hook.
// +kubebuilder:object:root=true
type InPlaceUpdateRequest struct {
	metav1.TypeMeta `json:",inline"`

	// CurrentMachine represents the current state of the machine undergoing in-place update.
	CurrentMachine clusterv1.Machine `json:"currentMachine"`

	// DesiredMachine represents the desired state of the machine after the update.
	DesiredMachine clusterv1.Machine `json:"desiredMachine"`

	// UpdateConfig contains configuration for the in-place update.
	UpdateConfig InPlaceUpdateConfig `json:"updateConfig"`
}

// InPlaceUpdateConfig defines the parameters for an in-place update.
type InPlaceUpdateConfig struct {
	// TODO
}

var _ RetryResponseObject = &InPlaceUpdateResponse{}

// InPlaceUpdateResponse is the response of the InPlaceUpdate hook.
// +kubebuilder:object:root=true
type InPlaceUpdateResponse struct {
	metav1.TypeMeta `json:",inline"`

	// CommonRetryResponse contains Status, Message and RetryAfterSeconds fields.
	CommonRetryResponse `json:",inline"`
}

// InPlaceUpdate is the hook that will be called to handle in-place updates.
func InPlaceUpdate(*InPlaceUpdateRequest, *InPlaceUpdateResponse) {}

func init() {
	catalogBuilder.RegisterHook(InPlaceUpdate, &runtimecatalog.HookMeta{
		Tags:    []string{"In-place Update Hooks"},
		Summary: "Cluster API Runtime will call this hook to process in-place updates",
		Description: "Cluster API Runtime will call this hook whenever a Machine undergoes an in-place update.\n" +
			"\n" +
			"Notes:\n" +
			"- This hook will be called for Machines undergoing an in-place update\n" +
			"- The call's request contains the CurrentMachine and DesiredMachine objects along with update configuration\n" +
			"- This is a blocking hook; Runtime Extension implementers can use this hook to execute\n" +
			"tasks required for a successful in-place update.",
	})
}
