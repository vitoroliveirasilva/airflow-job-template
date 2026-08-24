# Isolated RPA pattern

`example.py` keeps the full mutable browser transaction in one logical task, checks remote state before changing it, mutates only from the expected state, validates the result, captures a caller-chosen diagnostic artifact on failure, and closes the session in `finally`.

The validation failure is retryable because every attempt begins by checking whether the remote mutation already landed. Unknown states fail without mutation. A session-cleanup failure after a confirmed remote update is logged instead of turning a successful side effect into a task retry.

Run browser/Java/SAP GUI jobs in a prepared worker, `@task.external_python`, Docker, Kubernetes, or a provider-native operator that actually exists in your infrastructure. Use a pool/queue only when that pool/queue has been provisioned. Browser drivers are intentionally not core dependencies.
