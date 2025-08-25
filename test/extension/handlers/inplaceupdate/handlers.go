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

// Package inplaceupdate contains the handlers for the in-place Kubernetes version update hooks.
//
// The implementation of the handlers is specifically designed for Cluster API E2E tests use cases,
// focusing on simulating realistic Kubernetes version upgrades through the in-place update mechanism.
// When implementing custom RuntimeExtension, it is only required to expose HandlerFunc with the
// signature defined in sigs.k8s.io/cluster-api/api/runtime/hooks/v1alpha1.
package inplaceupdate

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/pkg/errors"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/yaml"

	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	runtimehooksv1 "sigs.k8s.io/cluster-api/api/runtime/hooks/v1alpha1"
)

const (
	extensionConfigNameKey        = "extensionConfigName"
	updateProgressConfigMapPrefix = "test-extension-k8s-upgrade-"
	defaultUpgradeDuration        = 60 * time.Second
)

type ExtensionHandlers struct {
	client client.Client
}

func NewExtensionHandlers(client client.Client) *ExtensionHandlers {
	return &ExtensionHandlers{
		client: client,
	}
}

func (e *ExtensionHandlers) DoCanUpdateMachine(ctx context.Context, request *runtimehooksv1.CanUpdateMachineRequest, response *runtimehooksv1.CanUpdateMachineResponse) {
	log := ctrl.LoggerFrom(ctx)
	log.Info("CanUpdateMachine hook called for K8s version updates", "changes", request.Changes)

	acceptedChanges := []string{}

	for _, change := range request.Changes {
		switch {
		case strings.Contains(change, "machine.spec.version"):
			acceptedChanges = append(acceptedChanges, change)
			log.V(5).Info("Accepting machine version change", "change", change)
		default:
			log.V(5).Info("Cannot handle non-Kubernetes change", "change", change)
		}
	}

	response.Status = runtimehooksv1.ResponseStatusSuccess

	if len(acceptedChanges) > 0 {
		response.Message = strings.Join(acceptedChanges, ",")
		log.Info("CanUpdateMachine responded positively for K8s updates", "acceptedChanges", acceptedChanges)
	} else {
		response.Message = ""
		log.Info("CanUpdateMachine responded negatively - no Kubernetes version changes found")
	}
}

func (e *ExtensionHandlers) DoUpdateMachine(ctx context.Context, request *runtimehooksv1.UpdateMachineRequest, response *runtimehooksv1.UpdateMachineResponse) {
	log := ctrl.LoggerFrom(ctx).WithValues("machine", klog.KRef(request.MachineRef.Namespace, request.MachineRef.Name))
	ctx = ctrl.LoggerInto(ctx, log)
	log.Info("UpdateMachine hook called for Kubernetes version update")

	machine := &clusterv1.Machine{}
	machineKey := types.NamespacedName{
		Namespace: request.MachineRef.Namespace,
		Name:      request.MachineRef.Name,
	}

	if err := e.client.Get(ctx, machineKey, machine); err != nil {
		log.Error(err, "Failed to get machine")
		response.Status = runtimehooksv1.ResponseStatusFailure
		response.Message = fmt.Sprintf("Failed to get machine: %v", err)
		return
	}

	if machine.Spec.Version != "" {
		log.Info("Updating machine Kubernetes version", "targetVersion", machine.Spec.Version)
	}

	progressKey := updateProgressConfigMapPrefix + request.MachineRef.Name
	updateStatus, err := e.getUpdateProgress(ctx, request.MachineRef.Namespace, progressKey)
	if err != nil {
		log.Error(err, "Failed to get update progress")
		response.Status = runtimehooksv1.ResponseStatusFailure
		response.Message = fmt.Sprintf("Failed to get update progress: %v", err)
		return
	}

	if updateStatus == nil || updateStatus.TargetVersion != machine.Spec.Version {
		if updateStatus != nil {
			log.Info("Target version changed, resetting upgrade progress", "oldVersion", updateStatus.TargetVersion, "newVersion", machine.Spec.Version)
		}

		if err := e.initializeUpdate(ctx, request.MachineRef.Namespace, progressKey, machine); err != nil {
			log.Error(err, "Failed to initialize Kubernetes version update")
			response.Status = runtimehooksv1.ResponseStatusFailure
			response.Message = fmt.Sprintf("Failed to initialize Kubernetes version update: %v", err)
			return
		}

		response.Status = runtimehooksv1.ResponseStatusSuccess
		response.Message = "Kubernetes version update started - draining node and preparing for upgrade"
		response.RetryAfterSeconds = 5
		log.Info("Kubernetes version update initialized", "retryAfter", 5, "targetVersion", machine.Spec.Version)
		return
	}

	elapsed := time.Since(updateStatus.StartTime)

	switch updateStatus.Phase {
	case "initializing":
		if elapsed < 20*time.Second {
			response.Status = runtimehooksv1.ResponseStatusSuccess
			response.Message = "Draining node and preparing for Kubernetes upgrade"
			response.RetryAfterSeconds = 5
			log.Info("Kubernetes upgrade in progress - draining node", "elapsed", elapsed, "retryAfter", 5)
			return
		}

		updateStatus.Phase = "upgrading-kubelet"
		updateStatus.Message = "Upgrading kubelet and kubeadm"
		log.Info("Kubernetes upgrade phase transition", "phase", "upgrading-kubelet", "targetVersion", machine.Spec.Version)
		if err := e.saveUpdateProgress(ctx, request.MachineRef.Namespace, progressKey, updateStatus); err != nil {
			log.Error(err, "Failed to save update progress")
		}

	case "upgrading-kubelet":
		if elapsed < 40*time.Second {
			response.Status = runtimehooksv1.ResponseStatusSuccess
			response.Message = "Upgrading kubelet, kubeadm, and kubectl packages"
			response.RetryAfterSeconds = 5
			log.Info("Kubernetes upgrade in progress - upgrading kubelet", "elapsed", elapsed, "retryAfter", 5)
			return
		}

		updateStatus.Phase = "validating"
		updateStatus.Message = "Validating Kubernetes upgrade and rejoining cluster"
		log.Info("Kubernetes upgrade phase transition", "phase", "validating", "targetVersion", machine.Spec.Version)
		if err := e.saveUpdateProgress(ctx, request.MachineRef.Namespace, progressKey, updateStatus); err != nil {
			log.Error(err, "Failed to save update progress")
		}

	case "validating":
		if elapsed < 60*time.Second {
			response.Status = runtimehooksv1.ResponseStatusSuccess
			response.Message = "Validating Kubernetes upgrade, uncordoning node, and running health checks"
			response.RetryAfterSeconds = 5
			log.Info("Kubernetes upgrade in progress - validating and uncordoning", "elapsed", elapsed, "retryAfter", 5)
			return
		}

		updateStatus.Phase = "completed"
		updateStatus.Message = "Kubernetes version upgrade completed successfully"
	}

	if err := e.cleanupUpdateProgress(ctx, request.MachineRef.Namespace, progressKey); err != nil {
		log.Error(err, "Failed to cleanup update progress")
	}

	response.Status = runtimehooksv1.ResponseStatusSuccess
	response.Message = fmt.Sprintf("Kubernetes version upgrade completed successfully to version %s", machine.Spec.Version)
	response.RetryAfterSeconds = 0
	log.Info("Kubernetes version upgrade completed successfully", "targetVersion", machine.Spec.Version, "totalTime", elapsed)
}

type UpdateProgress struct {
	StartTime     time.Time `json:"startTime"`
	Phase         string    `json:"phase"`
	Message       string    `json:"message"`
	Machine       string    `json:"machine"`
	TargetVersion string    `json:"targetVersion,omitempty"`
}

func (e *ExtensionHandlers) getUpdateProgress(ctx context.Context, namespace, name string) (*UpdateProgress, error) {
	cm := &corev1.ConfigMap{}
	key := types.NamespacedName{Namespace: namespace, Name: name}

	if err := e.client.Get(ctx, key, cm); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, errors.Wrapf(err, "failed to get progress ConfigMap %s", name)
	}

	progressData, exists := cm.Data["progress"]
	if !exists {
		return nil, nil
	}

	var progress UpdateProgress
	if err := yaml.Unmarshal([]byte(progressData), &progress); err != nil {
		return nil, errors.Wrapf(err, "failed to unmarshal progress data")
	}

	return &progress, nil
}

func (e *ExtensionHandlers) initializeUpdate(ctx context.Context, namespace, name string, machine *clusterv1.Machine) error {
	progress := &UpdateProgress{
		StartTime:     time.Now(),
		Phase:         "initializing",
		Message:       "Starting Kubernetes version upgrade",
		Machine:       machine.Name,
		TargetVersion: machine.Spec.Version,
	}

	return e.saveUpdateProgress(ctx, namespace, name, progress)
}

func (e *ExtensionHandlers) saveUpdateProgress(ctx context.Context, namespace, name string, progress *UpdateProgress) error {
	progressData, err := yaml.Marshal(progress)
	if err != nil {
		return errors.Wrapf(err, "failed to marshal progress data")
	}

	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Data: map[string]string{
			"progress": string(progressData),
		},
	}

	if err := e.client.Create(ctx, cm); err != nil {
		if apierrors.IsAlreadyExists(err) {
			existing := &corev1.ConfigMap{}
			key := types.NamespacedName{Namespace: namespace, Name: name}
			if err := e.client.Get(ctx, key, existing); err != nil {
				return errors.Wrapf(err, "failed to get existing ConfigMap")
			}
			existing.Data = cm.Data
			if err := e.client.Update(ctx, existing); err != nil {
				return errors.Wrapf(err, "failed to update progress ConfigMap")
			}
		} else {
			return errors.Wrapf(err, "failed to create progress ConfigMap")
		}
	}

	return nil
}

func (e *ExtensionHandlers) cleanupUpdateProgress(ctx context.Context, namespace, name string) error {
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
	}

	if err := e.client.Delete(ctx, cm); err != nil && !apierrors.IsNotFound(err) {
		return errors.Wrapf(err, "failed to delete progress ConfigMap")
	}

	return nil
}
