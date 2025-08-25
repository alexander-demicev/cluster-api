/*
Copyright 2018 The Kubernetes Authors.

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

package machinedeployment

import (
	"context"
	"sort"
	"strings"

	"github.com/pkg/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog/v2"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	runtimehooksv1 "sigs.k8s.io/cluster-api/api/runtime/hooks/v1alpha1"
	"sigs.k8s.io/cluster-api/feature"
	"sigs.k8s.io/cluster-api/internal/controllers/machinedeployment/mdutil"
	"sigs.k8s.io/cluster-api/internal/hooks"
	"sigs.k8s.io/cluster-api/util/annotations"
	"sigs.k8s.io/cluster-api/util/patch"
)

// rolloutRolling implements the logic for rolling a new MachineSet.
func (r *Reconciler) rolloutRolling(ctx context.Context, md *clusterv1.MachineDeployment, msList []*clusterv1.MachineSet, templateExists bool) error {
	log := ctrl.LoggerFrom(ctx)

	useExternalUpdate := false
	if feature.Gates.Enabled(feature.InPlaceUpdates) && len(msList) > 0 {
		log.Info("InPlaceUpdates feature gate enabled, checking for external update capability", "existingMachineSets", len(msList))
		canUseExternalUpdate, err := r.canMachineSetsUseExternalUpdate(ctx, msList, md.Spec.Template.Spec)
		if err != nil {
			log.Error(err, "Failed to check external update capability")
			return err
		}
		useExternalUpdate = canUseExternalUpdate
	}

	newMS, oldMSs, err := r.getAllMachineSetsAndSyncRevision(ctx, md, msList, true, templateExists, useExternalUpdate)
	if err != nil {
		return err
	}

	// newMS can be nil in case there is already a MachineSet associated with this deployment,
	// but there are only either changes in annotations or MinReadySeconds. Or in other words,
	// this can be nil if there are changes, but no replacement of existing machines is needed.
	if newMS == nil {
		return nil
	}

	if useExternalUpdate {
		if err := r.executeExternalUpdateStrategy(ctx, oldMSs, newMS); err != nil {
			return err
		}
	}

	allMSs := append(oldMSs, newMS)

	// Scale up, if we can.
	if err := r.reconcileNewMachineSet(ctx, allMSs, newMS, md); err != nil {
		return err
	}

	if err := r.syncDeploymentStatus(allMSs, newMS, md); err != nil {
		return err
	}

	// Scale down, if we can.
	if err := r.reconcileOldMachineSets(ctx, allMSs, oldMSs, newMS, md); err != nil {
		return err
	}

	if err := r.syncDeploymentStatus(allMSs, newMS, md); err != nil {
		return err
	}

	if mdutil.DeploymentComplete(md, &md.Status) {
		if err := r.cleanupDeployment(ctx, oldMSs, md); err != nil {
			return err
		}
	}

	return nil
}

func (r *Reconciler) reconcileNewMachineSet(ctx context.Context, allMSs []*clusterv1.MachineSet, newMS *clusterv1.MachineSet, deployment *clusterv1.MachineDeployment) error {
	if err := r.cleanupDisableMachineCreateAnnotation(ctx, newMS); err != nil {
		return err
	}

	if deployment.Spec.Replicas == nil {
		return errors.Errorf("spec.replicas for MachineDeployment %v is nil, this is unexpected", client.ObjectKeyFromObject(deployment))
	}

	if newMS.Spec.Replicas == nil {
		return errors.Errorf("spec.replicas for MachineSet %v is nil, this is unexpected", client.ObjectKeyFromObject(newMS))
	}

	if *(newMS.Spec.Replicas) == *(deployment.Spec.Replicas) {
		// Scaling not required.
		return nil
	}

	if *(newMS.Spec.Replicas) > *(deployment.Spec.Replicas) {
		// Scale down.
		return r.scaleMachineSet(ctx, newMS, *(deployment.Spec.Replicas), deployment)
	}

	newReplicasCount, err := mdutil.NewMSNewReplicas(deployment, allMSs, *newMS.Spec.Replicas)
	if err != nil {
		return err
	}
	return r.scaleMachineSet(ctx, newMS, newReplicasCount, deployment)
}

func (r *Reconciler) reconcileOldMachineSets(ctx context.Context, allMSs []*clusterv1.MachineSet, oldMSs []*clusterv1.MachineSet, newMS *clusterv1.MachineSet, deployment *clusterv1.MachineDeployment) error {
	log := ctrl.LoggerFrom(ctx)

	if deployment.Spec.Replicas == nil {
		return errors.Errorf("spec.replicas for MachineDeployment %v is nil, this is unexpected",
			client.ObjectKeyFromObject(deployment))
	}

	if newMS.Spec.Replicas == nil {
		return errors.Errorf("spec.replicas for MachineSet %v is nil, this is unexpected",
			client.ObjectKeyFromObject(newMS))
	}

	oldMachinesCount := mdutil.GetReplicaCountForMachineSets(oldMSs)
	if oldMachinesCount == 0 {
		// Can't scale down further
		return nil
	}

	allMachinesCount := mdutil.GetReplicaCountForMachineSets(allMSs)
	log.V(4).Info("New MachineSet has available machines",
		"machineset", client.ObjectKeyFromObject(newMS).String(), "available-replicas", ptr.Deref(newMS.Status.AvailableReplicas, 0))
	maxUnavailable := mdutil.MaxUnavailable(*deployment)

	// Check if we can scale down. We can scale down in the following 2 cases:
	// * Some old MachineSets have unhealthy replicas, we could safely scale down those unhealthy replicas since that won't further
	//  increase unavailability.
	// * New MachineSet has scaled up and it's replicas becomes ready, then we can scale down old MachineSets in a further step.
	//
	// maxScaledDown := allMachinesCount - minAvailable - newMachineSetMachinesUnavailable
	// take into account not only maxUnavailable and any surge machines that have been created, but also unavailable machines from
	// the newMS, so that the unavailable machines from the newMS would not make us scale down old MachineSets in a further
	// step(that will increase unavailability).
	//
	// Concrete example:
	//
	// * 10 replicas
	// * 2 maxUnavailable (absolute number, not percent)
	// * 3 maxSurge (absolute number, not percent)
	//
	// case 1:
	// * Deployment is updated, newMS is created with 3 replicas, oldMS is scaled down to 8, and newMS is scaled up to 5.
	// * The new MachineSet machines crashloop and never become available.
	// * allMachinesCount is 13. minAvailable is 8. newMSMachinesUnavailable is 5.
	// * A node fails and causes one of the oldMS machines to become unavailable. However, 13 - 8 - 5 = 0, so the oldMS won't be scaled down.
	// * The user notices the crashloop and does kubectl rollout undo to rollback.
	// * newMSMachinesUnavailable is 1, since we rolled back to the good MachineSet, so maxScaledDown = 13 - 8 - 1 = 4. 4 of the crashlooping machines will be scaled down.
	// * The total number of machines will then be 9 and the newMS can be scaled up to 10.
	//
	// case 2:
	// Same example, but pushing a new machine template instead of rolling back (aka "roll over"):
	// * The new MachineSet created must start with 0 replicas because allMachinesCount is already at 13.
	// * However, newMSMachinesUnavailable would also be 0, so the 2 old MachineSets could be scaled down by 5 (13 - 8 - 0), which would then
	// allow the new MachineSet to be scaled up by 5.
	availableReplicas := ptr.Deref(newMS.Status.AvailableReplicas, 0)

	minAvailable := *(deployment.Spec.Replicas) - maxUnavailable
	newMSUnavailableMachineCount := *(newMS.Spec.Replicas) - availableReplicas
	maxScaledDown := allMachinesCount - minAvailable - newMSUnavailableMachineCount
	if maxScaledDown <= 0 {
		return nil
	}

	// Clean up unhealthy replicas first, otherwise unhealthy replicas will block deployment
	// and cause timeout. See https://github.com/kubernetes/kubernetes/issues/16737
	oldMSs, cleanupCount, err := r.cleanupUnhealthyReplicas(ctx, oldMSs, deployment, maxScaledDown)
	if err != nil {
		return err
	}

	log.V(4).Info("Cleaned up unhealthy replicas from old MachineSets", "count", cleanupCount)

	// Scale down old MachineSets, need check maxUnavailable to ensure we can scale down
	allMSs = oldMSs
	allMSs = append(allMSs, newMS)
	scaledDownCount, err := r.scaleDownOldMachineSetsForRollingUpdate(ctx, allMSs, oldMSs, deployment)
	if err != nil {
		return err
	}

	log.V(4).Info("Scaled down old MachineSets of MachineDeployment", "count", scaledDownCount)
	return nil
}

// cleanupUnhealthyReplicas will scale down old MachineSets with unhealthy replicas, so that all unhealthy replicas will be deleted.
func (r *Reconciler) cleanupUnhealthyReplicas(ctx context.Context, oldMSs []*clusterv1.MachineSet, deployment *clusterv1.MachineDeployment, maxCleanupCount int32) ([]*clusterv1.MachineSet, int32, error) {
	log := ctrl.LoggerFrom(ctx)

	sort.Sort(mdutil.MachineSetsByCreationTimestamp(oldMSs))

	// Scale down all old MachineSets with any unhealthy replicas. MachineSet will honour spec.deletion.order
	// for deleting Machines. Machines with a deletion timestamp, with a failure message or without a nodeRef
	// are preferred for all strategies.
	// This results in a best effort to remove machines backing unhealthy nodes.
	totalScaledDown := int32(0)

	for _, targetMS := range oldMSs {
		if targetMS.Spec.Replicas == nil {
			return nil, 0, errors.Errorf("spec.replicas for MachineSet %v is nil, this is unexpected", client.ObjectKeyFromObject(targetMS))
		}

		if totalScaledDown >= maxCleanupCount {
			break
		}

		oldMSReplicas := *(targetMS.Spec.Replicas)
		if oldMSReplicas == 0 {
			// cannot scale down this MachineSet.
			continue
		}

		oldMSAvailableReplicas := ptr.Deref(targetMS.Status.AvailableReplicas, 0)
		log.V(4).Info("Found available Machines in old MachineSet",
			"count", oldMSAvailableReplicas, "target-machineset", client.ObjectKeyFromObject(targetMS).String())
		if oldMSReplicas == oldMSAvailableReplicas {
			// no unhealthy replicas found, no scaling required.
			continue
		}

		remainingCleanupCount := maxCleanupCount - totalScaledDown
		unhealthyCount := oldMSReplicas - oldMSAvailableReplicas
		scaledDownCount := min(remainingCleanupCount, unhealthyCount)
		newReplicasCount := oldMSReplicas - scaledDownCount

		if newReplicasCount > oldMSReplicas {
			return nil, 0, errors.Errorf("when cleaning up unhealthy replicas, got invalid request to scale down %v: %d -> %d",
				client.ObjectKeyFromObject(targetMS), oldMSReplicas, newReplicasCount)
		}

		if err := r.scaleMachineSet(ctx, targetMS, newReplicasCount, deployment); err != nil {
			return nil, totalScaledDown, err
		}

		totalScaledDown += scaledDownCount
	}

	return oldMSs, totalScaledDown, nil
}

// scaleDownOldMachineSetsForRollingUpdate scales down old MachineSets when deployment strategy is "RollingUpdate".
// Need check maxUnavailable to ensure availability.
func (r *Reconciler) scaleDownOldMachineSetsForRollingUpdate(ctx context.Context, allMSs []*clusterv1.MachineSet, oldMSs []*clusterv1.MachineSet, deployment *clusterv1.MachineDeployment) (int32, error) {
	log := ctrl.LoggerFrom(ctx)

	if deployment.Spec.Replicas == nil {
		return 0, errors.Errorf("spec.replicas for MachineDeployment %v is nil, this is unexpected", client.ObjectKeyFromObject(deployment))
	}

	maxUnavailable := mdutil.MaxUnavailable(*deployment)
	minAvailable := *(deployment.Spec.Replicas) - maxUnavailable

	// Find the number of available machines.
	availableMachineCount := ptr.Deref(mdutil.GetAvailableReplicaCountForMachineSets(allMSs), 0)

	// Check if we can scale down.
	if availableMachineCount <= minAvailable {
		// Cannot scale down.
		return 0, nil
	}

	log.V(4).Info("Found available machines in deployment, scaling down old MSes", "count", availableMachineCount)

	sort.Sort(mdutil.MachineSetsByCreationTimestamp(oldMSs))

	totalScaledDown := int32(0)
	totalScaleDownCount := availableMachineCount - minAvailable
	for _, targetMS := range oldMSs {
		if targetMS.Spec.Replicas == nil {
			return 0, errors.Errorf("spec.replicas for MachineSet %v is nil, this is unexpected", client.ObjectKeyFromObject(targetMS))
		}

		if totalScaledDown >= totalScaleDownCount {
			// No further scaling required.
			break
		}

		if *(targetMS.Spec.Replicas) == 0 {
			// cannot scale down this MachineSet.
			continue
		}

		// Scale down.
		scaleDownCount := min(*(targetMS.Spec.Replicas), totalScaleDownCount-totalScaledDown)
		newReplicasCount := *(targetMS.Spec.Replicas) - scaleDownCount
		if newReplicasCount > *(targetMS.Spec.Replicas) {
			return totalScaledDown, errors.Errorf("when scaling down old MachineSet, got invalid request to scale down %v: %d -> %d",
				client.ObjectKeyFromObject(targetMS), *(targetMS.Spec.Replicas), newReplicasCount)
		}

		if err := r.scaleMachineSet(ctx, targetMS, newReplicasCount, deployment); err != nil {
			return totalScaledDown, err
		}

		totalScaledDown += scaleDownCount
	}

	return totalScaledDown, nil
}

// cleanupDisableMachineCreateAnnotation will remove the disable machine create annotation from new MachineSets that were created during reconcileOldMachineSetsOnDelete.
func (r *Reconciler) cleanupDisableMachineCreateAnnotation(ctx context.Context, newMS *clusterv1.MachineSet) error {
	log := ctrl.LoggerFrom(ctx, "MachineSet", klog.KObj(newMS))

	if newMS.Annotations != nil {
		if _, ok := newMS.Annotations[clusterv1.DisableMachineCreateAnnotation]; ok {
			log.V(4).Info("removing annotation on latest MachineSet to enable machine creation")
			patchHelper, err := patch.NewHelper(newMS, r.Client)
			if err != nil {
				return err
			}
			delete(newMS.Annotations, clusterv1.DisableMachineCreateAnnotation)
			err = patchHelper.Patch(ctx, newMS)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

// getMachinesForMachineSet returns machines for a given MachineSet.
func (r *Reconciler) getMachinesForMachineSet(ctx context.Context, ms *clusterv1.MachineSet) ([]*clusterv1.Machine, error) {
	selector, err := metav1.LabelSelectorAsSelector(&ms.Spec.Selector)
	if err != nil {
		return nil, err
	}

	machineList := &clusterv1.MachineList{}
	if err := r.Client.List(ctx, machineList, client.InNamespace(ms.Namespace), client.MatchingLabelsSelector{Selector: selector}); err != nil {
		return nil, err
	}

	var machines []*clusterv1.Machine
	for i := range machineList.Items {
		machines = append(machines, &machineList.Items[i])
	}

	return machines, nil
}

func (r *Reconciler) calculateMachineSpecChanges(currentMachine *clusterv1.Machine, newSpec clusterv1.MachineSpec) ([]string, error) {
	var changes []string

	if currentMachine.Spec.Version != newSpec.Version {
		changes = append(changes, "machine.spec.version")
	}

	// TODO: Add more fields to compare.

	return changes, nil
}

func (r *Reconciler) canMachineSetsUseExternalUpdate(ctx context.Context, machineSets []*clusterv1.MachineSet, desiredSpec clusterv1.MachineSpec) (bool, error) {
	log := ctrl.LoggerFrom(ctx)
	log.Info("Checking if MachineSets can use external update", "machineSets", len(machineSets))

	var allMachines []*clusterv1.Machine
	for _, ms := range machineSets {
		machines, err := r.getMachinesForMachineSet(ctx, ms)
		if err != nil {
			return false, err
		}
		allMachines = append(allMachines, machines...)
	}

	if len(allMachines) == 0 {
		log.Info("No machines to update in MachineSets - external update not applicable")
		return false, nil
	}

	for _, machine := range allMachines {
		if hooks.IsPending(runtimehooksv1.ExternalUpdate, machine) {
			return false, errors.New("external update already in progress, waiting for completion")
		}
	}

	changes, err := r.calculateMachineSpecChanges(allMachines[0], desiredSpec)
	if err != nil {
		log.Error(err, "Failed to calculate machine spec changes")
		return false, err
	}

	if len(changes) == 0 {
		return false, nil
	}

	log.Info("Detected changes for MachineDeployment machines", "changes", changes)

	for _, machine := range allMachines {
		canUpdate, err := r.canMachineUseExternalUpdate(ctx, machine, desiredSpec, changes)
		if err != nil {
			return false, err
		}
		if !canUpdate {
			log.Info("Machine cannot be updated in-place, will fallback to rolling update",
				"machine", machine.Name)
			return false, nil
		}
	}

	log.Info("All machines can be updated in-place using external updaters",
		"machineCount", len(allMachines))
	return true, nil
}

// canAllMachinesUseExternalUpdate determines if all machines in the deployment can be updated using external updaters.
// This implements the proposal's deployment-level strategy decision for MachineDeployment.
func (r *Reconciler) canAllMachinesUseExternalUpdate(ctx context.Context, oldMSs []*clusterv1.MachineSet, newMS *clusterv1.MachineSet) (bool, error) {
	log := ctrl.LoggerFrom(ctx)

	// Collect all machines that need to be updated
	var allMachines []*clusterv1.Machine
	for _, ms := range oldMSs {
		machines, err := r.getMachinesForMachineSet(ctx, ms)
		if err != nil {
			return false, err
		}
		allMachines = append(allMachines, machines...)
	}

	if len(allMachines) == 0 {
		log.V(4).Info("No MachineDeployment machines to update")
		return true, nil
	}

	// Check if any machine is already pending external update
	// If so, we should wait for it to complete, not fall back to rolling updates
	for _, machine := range allMachines {
		if hooks.IsPending(runtimehooksv1.ExternalUpdate, machine) {
			log.V(4).Info("Machine is already pending external update, waiting for completion",
				"machine", machine.Name)
			// Return an error to requeue and wait for the external update to complete
			// We must not fall back to rolling updates when external updates are in progress
			return false, errors.New("external update already in progress, waiting for completion")
		}
	}

	// Calculate the required changes by comparing old and new MachineSet specs
	changes, err := r.calculateMachineSpecChanges(allMachines[0], newMS.Spec.Template.Spec)
	if err != nil {
		return false, err
	}

	if len(changes) == 0 {
		log.V(4).Info("No changes detected between old and new machine specs")
		return true, nil
	}

	log.V(4).Info("Detected changes for MachineDeployment machines", "changes", changes)

	// Check each machine to ensure ALL can be updated in-place
	for _, machine := range allMachines {
		canUpdate, err := r.canMachineUseExternalUpdate(ctx, machine, newMS.Spec.Template.Spec, changes)
		if err != nil {
			return false, err
		}
		if !canUpdate {
			log.V(4).Info("Machine cannot be updated in-place, will fallback to rolling update",
				"machine", machine.Name)
			return false, nil
		}
	}

	log.V(4).Info("All MachineDeployment machines can be updated in-place using external updaters",
		"machineCount", len(allMachines))
	return true, nil
}

func (r *Reconciler) executeExternalUpdateStrategy(ctx context.Context, oldMSs []*clusterv1.MachineSet, newMS *clusterv1.MachineSet) error {
	log := ctrl.LoggerFrom(ctx)

	changes, err := r.calculateMachineSpecChanges(&clusterv1.Machine{Spec: oldMSs[0].Spec.Template.Spec}, newMS.Spec.Template.Spec)
	if err != nil {
		return err
	}

	var movedMachines int32 = 0

	for _, ms := range oldMSs {
		if err := r.pauseMachineSet(ctx, ms, "external-update-in-progress"); err != nil {
			return errors.Wrapf(err, "failed to pause old MachineSet %s", ms.Name)
		}

		machines, err := r.getMachinesForMachineSet(ctx, ms)
		if err != nil {
			return err
		}

		var machinesToProcess []*clusterv1.Machine
		allMachinesAlreadyPending := true

		for _, machine := range machines {
			latestMachine := &clusterv1.Machine{}
			if err := r.Client.Get(ctx, client.ObjectKeyFromObject(machine), latestMachine); err != nil {
				return errors.Wrapf(err, "failed to get latest machine state for %s", machine.Name)
			}

			if hooks.IsPending(runtimehooksv1.ExternalUpdate, latestMachine) {
				log.V(5).Info("Machine is already pending external update, skipping",
					"machine", machine.Name)
			} else {
				machinesToProcess = append(machinesToProcess, machine)
				allMachinesAlreadyPending = false
			}
		}

		if allMachinesAlreadyPending && len(machines) > 0 {
			log.V(5).Info("All machines in MachineSet are already pending external update",
				"machineSet", ms.Name, "machineCount", len(machines))
			continue
		}

		for _, machine := range machinesToProcess {
			if err := r.markMachineForExternalUpdateAndMove(ctx, machine, newMS, ms, changes); err != nil {
				return err
			}
			movedMachines++
		}

		// Scale down the old MachineSet as machines are moved to the new one
		if *ms.Spec.Replicas > 0 {
			replicas := *ms.Spec.Replicas - int32(len(machines))
			if replicas < 0 {
				replicas = 0
			}

			patchHelper, err := patch.NewHelper(ms, r.Client)
			if err != nil {
				return err
			}
			ms.Spec.Replicas = &replicas
			if err := patchHelper.Patch(ctx, ms); err != nil {
				return err
			}
		}

		if err := r.unpauseMachineSet(ctx, ms); err != nil {
			log.Error(err, "Failed to unpause old MachineSet after external update", "machineSet", ms.Name)
		}
	}

	if err := r.unpauseMachineSet(ctx, newMS); err != nil {
		return errors.Wrapf(err, "failed to unpause new MachineSet %s after external update", newMS.Name)
	}

	log.Info("Successfully moved machines to new MachineSet for external update", "movedMachines", movedMachines, "newMachineSet", newMS.Name)
	return nil
}

func (r *Reconciler) canMachineUseExternalUpdate(ctx context.Context, machine *clusterv1.Machine, newSpec clusterv1.MachineSpec, changes []string) (bool, error) {
	log := ctrl.LoggerFrom(ctx)

	if r.RuntimeClient == nil {
		log.V(5).Info("RuntimeClient not configured, falling back to rolling update for machine", "machine", machine.Name)
		return false, errors.New("RuntimeClient not configured, cannot use external update for machine")
	}

	canUpdateRequest := &runtimehooksv1.CanUpdateMachineRequest{
		Changes: changes,
	}

	canUpdateResponse := &runtimehooksv1.CanUpdateMachineResponse{}
	if err := r.RuntimeClient.CallAllExtensions(ctx, runtimehooksv1.CanUpdateMachine, machine, canUpdateRequest, canUpdateResponse); err != nil {
		log.Error(err, "Failed to call CanUpdateMachine hook for machine", "machine", machine.Name)
		return false, errors.New("failed to call CanUpdateMachine hook for machine")
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
		log.V(4).Info("External updaters cannot handle all required changes for machine",
			"machine", machine.Name,
			"requestedChanges", changes,
			"acceptedChanges", acceptedChanges,
			"missingChanges", missingChanges,
			"responseMessage", canUpdateResponse.GetMessage())
		return false, nil
	}

	log.V(4).Info("External updaters can handle all required changes for machine",
		"machine", machine.Name,
		"requestedChanges", changes,
		"acceptedChanges", acceptedChanges,
		"responseMessage", canUpdateResponse.GetMessage())
	return true, nil
}

func (r *Reconciler) markMachineForExternalUpdateAndMove(ctx context.Context, machine *clusterv1.Machine, newMS *clusterv1.MachineSet, oldMS *clusterv1.MachineSet, changes []string) error {
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

	for _, change := range changes {
		switch change {
		case "machine.spec.version":
			machine.Spec.Version = newMS.Spec.Template.Spec.Version
		// Add other supported fields here as needed in the future
		default:
			log.Info("Skipping unsupported change during external update", "change", change)
		}
	}

	for i, ownerRef := range machine.OwnerReferences {
		if ownerRef.UID == oldMS.UID {
			machine.OwnerReferences = append(machine.OwnerReferences[:i], machine.OwnerReferences[i+1:]...)
			break
		}
	}

	newOwnerRef := metav1.OwnerReference{
		APIVersion: newMS.APIVersion,
		Kind:       newMS.Kind,
		Name:       newMS.Name,
		UID:        newMS.UID,
		Controller: ptr.To(true),
	}
	machine.OwnerReferences = append(machine.OwnerReferences, newOwnerRef)

	if machine.Labels == nil {
		machine.Labels = make(map[string]string)
	}
	for key, value := range newMS.Spec.Template.Labels {
		machine.Labels[key] = value
	}

	if err := patchHelper.Patch(ctx, machine); err != nil {
		log.Error(err, "Failed to patch machine with spec and changes annotation", "machine", machine.Name)
		return err
	}

	if err := hooks.MarkAsPending(ctx, r.Client, machine, runtimehooksv1.ExternalUpdate); err != nil {
		log.Error(err, "Failed to mark machine for external update", "machine", machine.Name)
		return err
	}

	log.Info("Marked machine for external update and moved to new MachineSet",
		"machine", machine.Name, "oldMachineSet", oldMS.Name, "newMachineSet", newMS.Name, "changes", changes)

	return nil
}

func (r *Reconciler) pauseMachineSet(ctx context.Context, ms *clusterv1.MachineSet, reason string) error {
	log := ctrl.LoggerFrom(ctx)

	if annotations.HasPaused(ms) {
		return nil
	}

	patchHelper, err := patch.NewHelper(ms, r.Client)
	if err != nil {
		return errors.Wrapf(err, "failed to create patch helper for MachineSet %s", ms.Name)
	}

	if ms.Annotations == nil {
		ms.Annotations = make(map[string]string)
	}
	ms.Annotations[clusterv1.PausedAnnotation] = reason

	if err := patchHelper.Patch(ctx, ms); err != nil {
		return errors.Wrapf(err, "failed to pause MachineSet %s", ms.Name)
	}

	log.Info("Paused MachineSet for external update", "machineSet", ms.Name, "reason", reason)
	return nil
}

func (r *Reconciler) unpauseMachineSet(ctx context.Context, ms *clusterv1.MachineSet) error {
	log := ctrl.LoggerFrom(ctx)

	if !annotations.HasPaused(ms) {
		return nil
	}

	patchHelper, err := patch.NewHelper(ms, r.Client)
	if err != nil {
		return errors.Wrapf(err, "failed to create patch helper for MachineSet %s", ms.Name)
	}

	if ms.Annotations != nil {
		delete(ms.Annotations, clusterv1.PausedAnnotation)
	}

	if err := patchHelper.Patch(ctx, ms); err != nil {
		return errors.Wrapf(err, "failed to unpause MachineSet %s", ms.Name)
	}

	log.Info("Unpaused MachineSet after external update", "machineSet", ms.Name)
	return nil
}
