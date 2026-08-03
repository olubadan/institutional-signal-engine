# Vast Instance Provisioning Record

- **Status:** Not provisioned — selected offer unavailable before charge
- **Verification timestamp:** `20260803T184627Z`
- **Requested offer ID:** `39005761`
- **Requested rental type:** On-demand
- **Requested image:** `ubuntu:24.04`
- **Requested disk:** `100 GB`
- **Expected CPU:** approximately Intel Core i7-11700
- **Expected vCPU:** `16`
- **Expected RAM:** approximately `64 GB`
- **Expected base price:** `$0.088/hour`, excluding storage and bandwidth
- **Vast instance ID:** Not assigned
- **Public host:** Not assigned
- **SSH port:** Not assigned
- **Actual image/disk/hourly price:** Not applicable

## Outcome

Both the normal exact-offer query and an exact-ID query with Vast's default
verified/non-external filters disabled returned an empty result. The offer's live
specifications and price could not be confirmed, so the required pre-charge gate
failed. No direct create-by-ID attempt was made, no alternative was selected, and
no charge was created.

Consequently, SSH configuration, repository cloning, documented bootstrap checks,
persistence testing, and deployment configuration were not started. This record
contains no credentials, keys, IP address, connection string, or one-time code.

See the sanitized chronological transcript at
[`docs/journal/vast-provisioning.log`](../journal/vast-provisioning.log).
