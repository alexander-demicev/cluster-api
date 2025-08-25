Next POC steps:

- `calculateMachineSpecChanges` and `calculateKCPMachineChanges` only detect that bootstrap or infra machine changed, not the individual fields that changed. TODO: add handling for that. They both also take the first available machine to calculate diff, is that safe?
- Check if any condition adjustments are needed for KCP/MD/MS objects.
- Decide if real update functionality can be added to the updater or extend with something else.
- Infra machine spec is immutable, find what to do with that.
- If updater is not reachable, should the controller fallback to rolling or keep trying in-place?
- The KCP controller completed its warmup before the test extension was even started.
- Use `DisableMachineCreateAnnotation` instead of pausing MachineSets?
- If multiple updaters are registered, the aggregated response message might be broken.
