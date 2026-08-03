# Vast.ai Candidate Report

- **Marketplace snapshot:** `20260803T182125Z`
- **Inspection mode:** Read-only; no instance was rented, provisioned, modified, or
  destroyed.
- **Authentication:** Vast CLI authentication succeeded. No account details or
  credential values were displayed or recorded.
- **Pricing basis:** Each hourly quote is Vast's `dph_total` from a search priced
  with `--storage 100`, so it includes the offer's compute charge and the quoted
  100 GB storage charge. The monthly estimate is `hourly price × 730`; usage-based
  network charges, taxes, and future marketplace price changes are excluded.
- **Compatibility screen:** Rentable and currently unrented; verified host;
  Ubuntu `24.04`; at least 4 effective vCPUs, 16 GB RAM, and 100 GB available
  storage; reliability at least `0.98`. Vast instances use container images, so
  the selected Ubuntu 24.04 image can support the required Docker-oriented
  workflow. Tool installation and clean-host reproducibility remain Phase 2
  provisioning acceptance checks.

Vast offers are volatile. Two otherwise attractive offers disappeared during the
inspection window. The three candidates below were reconfirmed in the final
snapshot; availability and price must be checked again immediately before any
owner-authorized rental.

## Ranked candidates

### 1. Offer 40176329 — recommended

- **Machine ID:** `138965`
- **Location:** Mexico (`MX`)
- **CPU:** Intel Core i9-14900KF; `32` effective vCPUs (`32` host logical CPUs
  listed)
- **RAM:** approximately `31.926 GB`
- **Available storage:** `669 GB`; quoted instance storage: `100 GB`
- **Measured network:** `194.6 Mbps` upload / `197.7 Mbps` download
- **Measured disk bandwidth:** `4509.0 MB/s`
- **Reliability:** `0.9957003` (`99.57003%`)
- **Host status:** verified; static IP available; `99` direct ports
- **Attached GPU:** RTX 4060 Ti, one GPU; not required by this project
- **Price with 100 GB storage:** **`$0.1344444444/hour`**
- **730-hour estimate:** **`$98.14/month`** (`$98.14444444` unrounded)
- **Strengths:** Modern high-throughput CPU, substantially more than the preferred
  CPU allocation, fast local disk, static IP, ample disk capacity, verified host,
  strong reliability, and North American placement for the US-market workload.
- **Selection rationale:** Best operational balance for a persistent real-time
  service. It costs more than the two alternatives, but avoids their static-IP and
  aging-CPU limitations while remaining below $100 for a 730-hour compute-plus-
  storage estimate.

### 2. Offer 45439601 — memory/network value

- **Machine ID:** `141620`
- **Location:** Ontario, Canada (`CA`)
- **CPU:** Intel Xeon E5-2680 v4; `14` effective vCPUs (`28` host logical CPUs
  listed)
- **RAM:** approximately `64.355 GB`
- **Available storage:** `128 GB`; quoted instance storage: `100 GB`
- **Measured network:** `2899.3 Mbps` upload / `2690.1 Mbps` download
- **Measured disk bandwidth:** `2586.0 MB/s`
- **Reliability:** `0.9949220` (`99.49220%`)
- **Host status:** verified; no static IP; `99` direct ports
- **Attached GPU:** RTX 2070, one GPU; not required by this project
- **Price with 100 GB storage:** **`$0.1077777778/hour`**
- **730-hour estimate:** **`$78.68/month`** (`$78.67777778` unrounded)
- **Strengths:** 64 GB RAM, 14 effective vCPUs, excellent measured networking,
  fast disk, economical pricing, and favorable proximity to US data services.
- **Selection rationale:** Strongest price-to-memory/network alternative, but it
  has only 28 GB of host storage headroom beyond the requested allocation, uses an
  older CPU generation, and does not advertise a static IP.

### 3. Offer 44217462 — lowest-cost option

- **Machine ID:** `141771`
- **Location:** Ontario, Canada (`CA`)
- **CPU:** Intel Core i7-7700K; `8` effective vCPUs (`8` host logical CPUs listed)
- **RAM:** approximately `32.051 GB`
- **Available storage:** `577 GB`; quoted instance storage: `100 GB`
- **Measured network:** `2151.8 Mbps` upload / `1899.5 Mbps` download
- **Measured disk bandwidth:** `1765.14 MB/s`
- **Reliability:** `0.9945227` (`99.45227%`)
- **Host status:** verified; no static IP; `198` direct ports
- **Attached GPU:** RTX 2070 Super, one GPU; not required by this project
- **Price with 100 GB storage:** **`$0.0944444444/hour`**
- **730-hour estimate:** **`$68.94/month`** (`$68.94444444` unrounded)
- **Strengths:** Meets the preferred CPU/RAM targets at the lowest confirmed
  price, with ample storage and excellent measured network throughput.
- **Selection rationale:** Best budget choice, but the older consumer CPU and lack
  of a static IP make it less suitable than the recommendation for a persistent,
  externally integrated service.

## Recommendation and owner decision

Select **offer `40176329` / machine `138965`** at the currently quoted
**`$0.1344444444/hour`**, estimated at **`$98.14/month`** for 730 hours including
100 GB storage. This is a recommendation only and creates no charge.

The owner must explicitly identify the selected offer or machine and authorize the
charge before any rental. Immediately before rental, revalidate that exact offer's
availability, price, Ubuntu 24.04 image compatibility, resource allocation, and
host indicators. If it has changed or disappeared, stop and obtain renewed owner
approval rather than silently substituting another offer.

## Availability update — 20260803T183142Z

The owner selected offer `40176329` / machine `138965` with a total-price ceiling
of `$0.14/hour` including 100 GB. Pre-charge queries at `20260803T183129Z` and
`20260803T183142Z` returned no record for the exact offer, including a second
query with default availability filters disabled. The offer is therefore no
longer available through the marketplace search endpoint. No rental or charge was
created, and no alternative was substituted.
