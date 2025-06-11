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
	"k8s.io/apimachinery/pkg/runtime"

	clusterv1beta1 "sigs.k8s.io/cluster-api/api/core/v1beta1"
	runtimecatalog "sigs.k8s.io/cluster-api/exp/runtime/catalog"
)

// CanUpdateMachineRequest is the request of the CanUpdateMachine hook.
// +kubebuilder:object:root=true
type CanUpdateMachineRequest struct {
	metav1.TypeMeta `json:",inline"`

	// CommonRequest contains fields common to all request types.
	CommonRequest `json:",inline"`

	// current contains the current state of the Machine and related objects.
	// +required
	Current CanUpdateMachineRequestObjects `json:"current"`

	// desired contains the desired state of the Machine and related objects.
	// +required
	Desired CanUpdateMachineRequestObjects `json:"desired"`
}

// CanUpdateMachineRequestObjects groups objects for CanUpdateMachineRequest.
type CanUpdateMachineRequestObjects struct {
	// machine is the full Machine object.
	// +required
	Machine clusterv1beta1.Machine `json:"machine"`

	// infrastructureMachine is the infra Machine object.
	// +required
	InfrastructureMachine runtime.RawExtension `json:"infrastructureMachine"`

	// bootstrapConfig is the bootstrap config object.
	// +optional
	BootstrapConfig *runtime.RawExtension `json:"bootstrapConfig,omitempty"`
}

// Patch is a single patch (JSONPatch or JSONMergePatch) which can include multiple operations.
type Patch struct {
	// patchType JSONPatch or JSONMergePatch.
	// +required
	PatchType PatchType `json:"patchType"`

	// patch data for the target object.
	// +required
	Patch []byte `json:"patch"`
}

var _ ResponseObject = &CanUpdateMachineResponse{}

// CanUpdateMachineResponse is the response of the CanUpdateMachine hook.
// +kubebuilder:object:root=true
type CanUpdateMachineResponse struct {
	metav1.TypeMeta `json:",inline"`

	// CommonResponse contains Status and Message fields common to all response types.
	CommonResponse `json:",inline"`

	// machinePatch, when applied to the current Machine spec, indicates changes handled in-place.
	// +optional
	MachinePatch *Patch `json:"machinePatch,omitempty"`

	// infrastructureMachinePatch indicates infra Machine spec changes handled in-place.
	// +optional
	InfrastructureMachinePatch *Patch `json:"infrastructureMachinePatch,omitempty"`

	// bootstrapConfigPatch indicates bootstrap config spec changes handled in-place.
	// +optional
	BootstrapConfigPatch *Patch `json:"bootstrapConfigPatch,omitempty"`
}

// CanUpdateMachine is the hook that will be called to determine if an extension
// can handle specific machine changes for in-place updates.
func CanUpdateMachine(*CanUpdateMachineRequest, *CanUpdateMachineResponse) {}

// CanUpdateMachineSetRequest is the request of the CanUpdateMachineSet hook.
// +kubebuilder:object:root=true
type CanUpdateMachineSetRequest struct {
	metav1.TypeMeta `json:",inline"`

	// CommonRequest contains fields common to all request types.
	CommonRequest `json:",inline"`

	// current contains the current MachineSet spec and related template objects.
	// +required
	Current CanUpdateMachineSetRequestObjects `json:"current"`

	// desired contains the desired MachineSet spec and related template objects.
	// +required
	Desired CanUpdateMachineSetRequestObjects `json:"desired"`
}

// CanUpdateMachineSetRequestObjects groups objects for CanUpdateMachineSetRequest.
type CanUpdateMachineSetRequestObjects struct {
	// machineSetSpec is the MachineSet spec.
	// +required
	MachineSetSpec clusterv1beta1.MachineSetSpec `json:"machineSetSpec"`

	// infrastructureMachineTemplateSpec contains the provider-specific infrastructure Machine template spec object.
	// +required
	InfrastructureMachineTemplateSpec runtime.RawExtension `json:"infrastructureMachineTemplateSpec"`

	// bootstrapConfigTemplateSpec contains the bootstrap configuration template spec object.
	// +optional
	BootstrapConfigTemplateSpec *runtime.RawExtension `json:"bootstrapConfigTemplateSpec,omitempty"`
}

// CanUpdateMachineSetResponse is the response of the CanUpdateMachineSet hook.
// +kubebuilder:object:root=true
type CanUpdateMachineSetResponse struct {
	metav1.TypeMeta `json:",inline"`

	// CommonResponse contains Status and Message fields common to all response types.
	CommonResponse `json:",inline"`

	// machineSetPatch indicates MachineSet spec changes handled in-place.
	// +optional
	MachineSetPatch *Patch `json:"machineSetPatch,omitempty"`

	// infrastructureMachineTemplateSpecPatch indicates infra template spec changes handled in-place.
	// +optional
	InfrastructureMachineTemplateSpecPatch *Patch `json:"infrastructureMachineTemplateSpecPatch,omitempty"`

	// bootstrapConfigTemplateSpecPatch indicates bootstrap template spec changes handled in-place.
	// +optional
	BootstrapConfigTemplateSpecPatch *Patch `json:"bootstrapConfigTemplateSpecPatch,omitempty"`
}

var _ ResponseObject = &CanUpdateMachineSetResponse{}

// CanUpdateMachineSet is the hook that will be called to determine if an extension
// can handle specific MachineSet changes for in-place updates.
func CanUpdateMachineSet(*CanUpdateMachineSetRequest, *CanUpdateMachineSetResponse) {}

// UpdateMachineRequest is the request of the UpdateMachine hook.
// +kubebuilder:object:root=true
type UpdateMachineRequest struct {
	metav1.TypeMeta `json:",inline"`

	// CommonRequest contains fields common to all request types.
	CommonRequest `json:",inline"`

	// desired contains the desired state of the Machine and related objects.
	// +required
	Desired UpdateMachineRequestObjects `json:"desired"`
}

var _ RetryResponseObject = &UpdateMachineResponse{}

// UpdateMachineResponse is the response of the UpdateMachine hook.
// The status of the update operation is determined by the CommonRetryResponse fields:
// - Status=Success + RetryAfterSeconds > 0: update is in progress
// - Status=Success + RetryAfterSeconds = 0: update completed successfully
// - Status=Failure: update failed
// +kubebuilder:object:root=true
type UpdateMachineResponse struct {
	metav1.TypeMeta `json:",inline"`

	// CommonRetryResponse contains Status, Message and RetryAfterSeconds fields.
	CommonRetryResponse `json:",inline"`
}

// UpdateMachineRequestObjects groups objects for UpdateMachineRequest.
type UpdateMachineRequestObjects struct {
	// machine is the full Machine object.
	// +required
	Machine clusterv1beta1.Machine `json:"machine"`

	// infrastructureMachine is the infra Machine object.
	// +required
	InfrastructureMachine runtime.RawExtension `json:"infrastructureMachine"`

	// bootstrapConfig is the bootstrap config object.
	// +optional
	BootstrapConfig *runtime.RawExtension `json:"bootstrapConfig,omitempty"`
}

// UpdateMachine is the hook that will be called to perform in-place updates on a machine.
// This hook should be idempotent and can be called multiple times for the same machine
// until it reports Done or Failed status.
func UpdateMachine(*UpdateMachineRequest, *UpdateMachineResponse) {}

func init() {
	catalogBuilder.RegisterHook(CanUpdateMachine, &runtimecatalog.HookMeta{
		Tags:    []string{"In-Place Update Hooks"},
		Summary: "Cluster API Runtime will call this hook to determine if an extension can handle specific machine changes",
		Description: "Called during update planning to determine if an extension can handle machine changes. " +
			"The request contains current and desired state spec for Machine, InfraMachine and optionally BootstrapConfig. " +
			"Extensions should return per-object patches to be applied on current objects to indicate which changes they can handle in-place.\n" +
			"\n" +
			"Notes:\n" +
			"- This hook is called during the planning phase of updates\n" +
			"- Only spec is provided, status fields are not included\n" +
			"- If no extension can cover the required changes, CAPI will fallback to rolling updates\n",
	})

	catalogBuilder.RegisterHook(CanUpdateMachineSet, &runtimecatalog.HookMeta{
		Tags:    []string{"In-Place Update Hooks"},
		Summary: "Cluster API Runtime will call this hook to determine if an extension can handle specific MachineSet changes",
		Description: "Called during update planning to determine if an extension can handle MachineSet changes. " +
			"The request contains current and desired state spec for MachineSet, InfraMachineTemplate and optionally BootstrapConfigTemplate. " +
			"Extensions should return per-object patches to be applied on current objects to indicate which changes they can handle in-place.",
	})

	catalogBuilder.RegisterHook(UpdateMachine, &runtimecatalog.HookMeta{
		Tags:    []string{"In-Place Update Hooks"},
		Summary: "Cluster API Runtime will call this hook to perform in-place updates on a machine",
		Description: "Cluster API Runtime will call this hook to perform the actual in-place update on a machine. " +
			"The request contains the desired state spec for Machine, InfraMachine and optionally BootstrapConfig. " +
			"The hook will be called repeatedly until it reports Done or Failed status.\n" +
			"\n" +
			"Notes:\n" +
			"- This hook must be idempotent - it can be called multiple times for the same machine\n",
	})
}
