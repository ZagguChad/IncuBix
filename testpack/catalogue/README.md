# Researched destination discovery catalogue

This folder contains 150 global-scope, English-language Discourse community records for AI and data, business and work, programming, design, writing, audio, and video audiences.

## Files

- `researched-global-english.csv` uses the application's supported catalogue-import columns. It is suitable for the bounded CSV import endpoint.
- `researched-global-english-provenance.csv` maps each destination URL to the public directory listing and the directory category/member/topic snapshot used during research.
- `destinations.csv` remains the small fictional `.example` test fixture. Do not use it as real catalogue data.

## Research limits

The records were selected from [ABDA's public directory of supported Discourse forums](https://betterdiscourse.app/forums/) on 23 November 2026. The directory labels these entries English (`data-lang=en`) and groups them by topic. Its member and topic figures are copied as a dated discovery snapshot; they are not independently audited audience estimates.

The user's “Global, English” choice defines the discovery scope. `geo=global` is a scope label, not a claim that each forum accepts users from every country. The catalogue is a researched lead list. The source listing does not establish that a forum is currently reachable, accepts new accounts, permits promotional posts, supports an API key for this operator, or allows automated posting. The provenance file marks reachability as `listed_only_not_checked`, posting rules as `not_reviewed`, permission as `not_confirmed`, and external connector certification as `not_performed`.

Review the current rules and obtain the forum owner's permission before any live publishing or certification. The service uses simulation by default and does not derive a posting policy from a directory listing.
