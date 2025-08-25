/*
Copyright 2020 The Kubernetes Authors.

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

package controllers

import (
	"context"
	"strings"

	"github.com/blang/semver/v4"
	"github.com/pkg/errors"
	"k8s.io/klog/v2"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	bootstrapv1 "sigs.k8s.io/cluster-api/api/bootstrap/kubeadm/v1beta2"
	controlplanev1 "sigs.k8s.io/cluster-api/api/controlplane/kubeadm/v1beta2"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	runtimehooksv1 "sigs.k8s.io/cluster-api/api/runtime/hooks/v1alpha1"
	"sigs.k8s.io/cluster-api/controlplane/kubeadm/internal"
	"sigs.k8s.io/cluster-api/feature"
	"sigs.k8s.io/cluster-api/internal/hooks"
	"sigs.k8s.io/cluster-api/util/collections"
	"sigs.k8s.io/cluster-api/util/patch"
)

func (r *KubeadmControlPlaneReconciler) upgradeControlPlane(
	ctx context.Context,
	controlPlane *internal.ControlPlane,
	machinesRequireUpgrade collections.Machines,
) (ctrl.Result, error) {
	logger := ctrl.LoggerFrom(ctx)

	// TODO: handle reconciliation of etcd members and kubeadm config in case they get out of sync with cluster

	workloadCluster, err := controlPlane.GetWorkloadCluster(ctx)
	if err != nil {
		logger.Error(err, "failed to get remote client for workload cluster", "Cluster", klog.KObj(controlPlane.Cluster))
		return ctrl.Result{}, err
	}

	parsedVersion, err := semver.ParseTolerant(controlPlane.KCP.Spec.Version)
	if err != nil {
		return ctrl.Result{}, errors.Wrapf(err, "failed to parse kubernetes version %q", controlPlane.KCP.Spec.Version)
	}

	// Ensure kubeadm clusterRoleBinding for v1.29+ as per https://github.com/kubernetes/kubernetes/pull/121305
	if err := workloadCluster.AllowClusterAdminPermissions(ctx, parsedVersion); err != nil {
		return ctrl.Result{}, errors.Wrap(err, "failed to set cluster-admin ClusterRoleBinding for kubeadm")
	}

	kubeadmCMMutators := make([]func(*bootstrapv1.ClusterConfiguration), 0)

	if controlPlane.KCP.Spec.KubeadmConfigSpec.ClusterConfiguration != nil {
		// Get the imageRepository or the correct value if nothing is set and a migration is necessary.
		imageRepository := internal.ImageRepositoryFromClusterConfig(controlPlane.KCP.Spec.KubeadmConfigSpec.ClusterConfiguration)

		kubeadmCMMutators = append(kubeadmCMMutators,
			workloadCluster.UpdateImageRepositoryInKubeadmConfigMap(imageRepository),
			workloadCluster.UpdateFeatureGatesInKubeadmConfigMap(controlPlane.KCP.Spec.KubeadmConfigSpec, parsedVersion),
			workloadCluster.UpdateAPIServerInKubeadmConfigMap(controlPlane.KCP.Spec.KubeadmConfigSpec.ClusterConfiguration.APIServer),
			workloadCluster.UpdateControllerManagerInKubeadmConfigMap(controlPlane.KCP.Spec.KubeadmConfigSpec.ClusterConfiguration.ControllerManager),
			workloadCluster.UpdateSchedulerInKubeadmConfigMap(controlPlane.KCP.Spec.KubeadmConfigSpec.ClusterConfiguration.Scheduler))

		// Etcd local and external are mutually exclusive and they cannot be switched, once set.
		if controlPlane.KCP.Spec.KubeadmConfigSpec.ClusterConfiguration.Etcd.Local != nil {
			kubeadmCMMutators = append(kubeadmCMMutators,
				workloadCluster.UpdateEtcdLocalInKubeadmConfigMap(controlPlane.KCP.Spec.KubeadmConfigSpec.ClusterConfiguration.Etcd.Local))
		} else {
			kubeadmCMMutators = append(kubeadmCMMutators,
				workloadCluster.UpdateEtcdExternalInKubeadmConfigMap(controlPlane.KCP.Spec.KubeadmConfigSpec.ClusterConfiguration.Etcd.External))
		}
	}

	// collectively update Kubeadm config map
	if err = workloadCluster.UpdateClusterConfiguration(ctx, parsedVersion, kubeadmCMMutators...); err != nil {
		return ctrl.Result{}, err
	}

	if feature.Gates.Enabled(feature.InPlaceUpdates) && len(machinesRequireUpgrade) > 0 {
		canUseExternalUpdate, err := r.canAllKCPMachinesUseExternalUpdate(ctx, controlPlane, machinesRequireUpgrade)
		if err != nil {
			return ctrl.Result{}, err
		}

		if canUseExternalUpdate {
			if err := r.executeKCPExternalUpdateStrategy(ctx, controlPlane, machinesRequireUpgrade); err != nil {
				return ctrl.Result{}, err
			}
			return ctrl.Result{}, nil
		}
	}

	switch controlPlane.KCP.Spec.Rollout.Strategy.Type {
	case controlplanev1.RollingUpdateStrategyType:
		// RolloutStrategy is currently defaulted and validated to be RollingUpdate
		// We can ignore MaxUnavailable because we are enforcing health checks before we get here.
		maxNodes := *controlPlane.KCP.Spec.Replicas + int32(controlPlane.KCP.Spec.Rollout.Strategy.RollingUpdate.MaxSurge.IntValue())
		if int32(controlPlane.Machines.Len()) < maxNodes {
			// scaleUp ensures that we don't continue scaling up while waiting for Machines to have NodeRefs
			return r.scaleUpControlPlane(ctx, controlPlane)
		}
		return r.scaleDownControlPlane(ctx, controlPlane, machinesRequireUpgrade)
	default:
		logger.Info("RolloutStrategy type is not set to RollingUpdate, unable to determine the strategy for rolling out machines")
		return ctrl.Result{}, nil
	}
}

func (r *KubeadmControlPlaneReconciler) canAllKCPMachinesUseExternalUpdate(ctx context.Context, controlPlane *internal.ControlPlane, machinesRequireUpgrade collections.Machines) (bool, error) {
	log := ctrl.LoggerFrom(ctx)

	if len(machinesRequireUpgrade) == 0 {
		log.V(5).Info("No KCP machines to update")
		return true, nil
	}

	for _, machine := range machinesRequireUpgrade {
		if hooks.IsPending(runtimehooksv1.UpdateMachine, machine) {
			log.V(5).Info("Machine is already pending external update, skipping external update strategy",
				"machine", machine.Name)
			return false, nil
		}
	}

	changes, err := r.calculateKCPMachineChanges(controlPlane, machinesRequireUpgrade)
	if err != nil {
		return false, err
	}

	if len(changes) == 0 {
		log.V(5).Info("No changes detected for KCP machines")
		return true, nil
	}

	log.V(5).Info("Detected changes for KCP machines", "changes", changes)

	for _, machine := range machinesRequireUpgrade {
		canUpdate, err := r.canKCPMachineUseExternalUpdate(ctx, machine, changes)
		if err != nil {
			return false, err
		}
		if !canUpdate {
			log.V(5).Info("KCP machine cannot be updated in-place, will fallback to rolling update",
				"machine", machine.Name)
			return false, nil
		}
	}

	log.V(5).Info("All KCP machines can be updated in-place using external updaters",
		"machineCount", len(machinesRequireUpgrade))
	return true, nil
}

func (r *KubeadmControlPlaneReconciler) calculateKCPMachineChanges(controlPlane *internal.ControlPlane, machinesRequireUpgrade collections.Machines) ([]string, error) {
	changes := []string{}

	if len(machinesRequireUpgrade) == 0 {
		return changes, nil
	}

	var sampleMachine *clusterv1.Machine // take the first machine as a sample since all KCP machines should have the same configuration
	for _, machine := range machinesRequireUpgrade {
		sampleMachine = machine
		break
	}

	if sampleMachine.Spec.Version != controlPlane.KCP.Spec.Version {
		changes = append(changes, "machine.spec.version")
	}

	// TODO: Add infrastructure template or bootstrap change detection for in-place updates

	return changes, nil
}

func (r *KubeadmControlPlaneReconciler) executeKCPExternalUpdateStrategy(ctx context.Context, controlPlane *internal.ControlPlane, machinesRequireUpgrade collections.Machines) error {
	log := ctrl.LoggerFrom(ctx)

	changes, err := r.calculateKCPMachineChanges(controlPlane, machinesRequireUpgrade)
	if err != nil {
		return err
	}

	for _, machine := range machinesRequireUpgrade {
		latestMachine := &clusterv1.Machine{}
		if err := r.Client.Get(ctx, client.ObjectKeyFromObject(machine), latestMachine); err != nil {
			return errors.Wrapf(err, "failed to get latest machine state for %s", machine.Name)
		}

		if hooks.IsPending(runtimehooksv1.UpdateMachine, latestMachine) {
			log.Info("Machine is already pending external update, waiting for completion",
				"machine", machine.Name)
			return nil
		}
	}

	for _, machine := range machinesRequireUpgrade {
		if err := r.markKCPMachineForExternalUpdate(ctx, machine, controlPlane, changes); err != nil {
			return err
		}

		log.Info("Prepared KCP machine for external update", "machine", machine.Name)

		// Only process one machine at a time for control plane stability
		// The controller will come back to process remaining machines in subsequent reconciliations
		// once this machine's update is complete
		break
	}

	return nil
}

func (r *KubeadmControlPlaneReconciler) canKCPMachineUseExternalUpdate(ctx context.Context, machine *clusterv1.Machine, changes []string) (bool, error) {
	log := ctrl.LoggerFrom(ctx)

	if r.RuntimeClient == nil {
		return false, errors.New("RuntimeClient not configured, cannot use external update for KCP machine")
	}

	canUpdateRequest := &runtimehooksv1.CanUpdateMachineRequest{
		Changes: changes,
	}

	canUpdateResponse := &runtimehooksv1.CanUpdateMachineResponse{}
	if err := r.RuntimeClient.CallAllExtensions(ctx, runtimehooksv1.CanUpdateMachine, machine, canUpdateRequest, canUpdateResponse); err != nil {
		log.Error(err, "Failed to call CanUpdateMachine hook for KCP machine", "machine", machine.Name)
		return false, errors.New("failed to call CanUpdateMachine hook for KCP machine")
	}

	acceptedChangesSet := make(map[string]bool)

	var acceptedChanges []string
	if message := canUpdateResponse.GetMessage(); message != "" {
		acceptedChanges = strings.Split(message, ",")
	}

	for _, change := range acceptedChanges {
		acceptedChangesSet[change] = true
	}

	var missingChanges []string
	for _, requiredChange := range changes {
		if !acceptedChangesSet[requiredChange] {
			missingChanges = append(missingChanges, requiredChange)
		}
	}

	if len(missingChanges) > 0 {
		log.V(5).Info("External updaters cannot handle all required changes for KCP machine",
			"machine", machine.Name,
			"requestedChanges", changes,
			"acceptedChanges", acceptedChanges,
			"missingChanges", missingChanges,
			"responseMessage", canUpdateResponse.GetMessage())
		return false, nil
	}

	log.V(5).Info("External updaters can handle all required changes for KCP machine",
		"machine", machine.Name,
		"requestedChanges", changes,
		"acceptedChanges", acceptedChanges,
		"responseMessage", canUpdateResponse.GetMessage())
	return true, nil
}

func (r *KubeadmControlPlaneReconciler) markKCPMachineForExternalUpdate(ctx context.Context, machine *clusterv1.Machine, controlPlane *internal.ControlPlane, changes []string) error {
	log := ctrl.LoggerFrom(ctx)

	patchHelper, err := patch.NewHelper(machine, r.Client)
	if err != nil {
		log.Error(err, "Failed to create patch helper for machine", "machine", machine.Name)
		return err
	}

	if machine.Annotations == nil {
		machine.Annotations = make(map[string]string)
	}
	machine.Annotations[clusterv1.ExternalUpdateChangesAnnotation] = strings.Join(changes, ",")

	machine.Spec.Version = controlPlane.KCP.Spec.Version

	if err := patchHelper.Patch(ctx, machine); err != nil {
		log.Error(err, "Failed to patch machine with version and changes annotation", "machine", machine.Name)
		return err
	}

	if err := hooks.MarkAsPending(ctx, r.Client, machine, runtimehooksv1.UpdateMachine); err != nil {
		log.Error(err, "Failed to mark KCP machine for external update", "machine", machine.Name)
		return err
	}

	log.Info("Marked KCP machine for external update", "machine", machine.Name, "targetVersion", controlPlane.KCP.Spec.Version, "changes", changes)

	return nil
}
