# Orthogonal Authz

## Executive Summary

Estuary's access control today is a tiered role model with only two tiers in practice: `read` for looking at data, and `admin` for everything else. That makes `admin` badly overloaded: platform engineers receive billing email alerts meant for finance, and the finance team has access to take down a production system.

This plan refactors the role hierarchy into fine-grained, independent capabilities — most immediately to support dedicated **billing** and **user management** capabilities, so customers can delegate those responsibilities without handing out platform admin.

## Technical Notes

- **Capabilities are a flat set, not a hierarchy.** The five capabilities — `read`, `write`, `admin`, `billing`, `user_management` — don't imply each other. An admin grant does not grant `billing`, and (once the migration completes) does not grant `write` either; each capability is listed explicitly. This is the whole point of the refactor, and has downstream consequences — most notably for publish-target checks, see Phases below.

  Once capabilities are orthogonal, the names `write` and `admin` start to feel vague — they were meaningful as tiers but don't describe a specific power on their own. A later migration phase renames and/or splits them (e.g. `write → publish`, or separating task control from catalog edits) once the shape and Postgrest retirement allow it.

- **Capabilities inherit down the prefix tree.** A grant at `acmeCo/` applies to every descendant prefix — `acmeCo/sales/`, `acmeCo/sales/leads/`, and so on. A user's effective capabilities on a given prefix are the union of every grant at that prefix or any ancestor. This is how scoping already works for `read`/`write`/`admin`, and the new capabilities inherit the same way.

  > The `billing` capability only really makes sense at the root prefix and will be inert on any subprefix; granting this capability on subprefixes will be inert. The UI can handle this as a special case.

- **Role grants narrow capabilities, never widen them.** When a user reaches a prefix through a role grant, their effective capabilities are the intersection of what the user has and what the role grant allows. Neither side can escalate past the other:

  | Alice's user grant on `acmeCo/` | `acmeCo/` role grant on `partner/shared/` | Alice's effective capabilities on `partner/shared/` |
  | ------------------------------- | ----------------------------------------- | --------------------------------------------------- |
  | `{read, write, billing}`        | `{read, write}`                           | `{read, write}` — `billing` is filtered out         |
  | `{read}`                        | `{read, write}`                           | `{read}` — the role grant can't add `write`         |

## Open Questions

1. **Do we need a `traverse` capability to gate role-grant traversal?**

   Today, only users with the `admin` role can traverse role grants at all. A read-only user on `acmeCo/` cannot follow a role grant from `acmeCo/` → `partner/shared/`.

   The role grant rule as stated in Technical Notes would change this. Once capabilities are orthogonal and we drop the `admin`-required gate, any user whose capabilities intersect with a role grant's capabilities can traverse it. That means every existing read-only user would suddenly gain read access to every prefix reachable through existing role grants — a potentially large, silent expansion of access.

   Should we add an explicit `traverse` capability to prevent this? With `traverse`, a user can only follow a role-grant edge if `traverse` appears on their user grant. `traverse` is a gate — it controls whether the user can enter the role grant at all, but it doesn't carry through to the effective capability set:

   | User grant on `acmeCo/` | Role grant `acmeCo/` → `partner/shared/` | Effective capabilities on `partner/shared/` |
   |---|---|---|
   | `{read, write}` | `{read, write}` | none — no `traverse` on user grant |
   | `{read, traverse}` | `{read, write}` | `{read}` — `traverse` lets her in, but `write` is filtered out because it wasn't on the user grant |

   We could backfill and add `traverse` wherever there is already an `admin` grant so as not to change anyone's existing level of access.

## Phases (still in progress)

We will interleave these phases with other changes (service accounts, better user management, billing features) as needed.

**Phase 1 — add the array, orthogonal capabilities only.** Introduce `capabilities capability[] NOT NULL DEFAULT '{}'` on `user_grants`. The existing `capability` enum stays authoritative for `read`/`write`/`admin`; the array only carries the new orthogonal capabilities (`billing`, `user_management`). Only the GraphQL/Rust path reads the array. This lets us gate `billing` and `user_management` features immediately without touching existing authz code paths.

**Phase 2 — dual-write the tiered capabilities into the array.** The array becomes authoritative for the Rust/GraphQL authz layer for all five capabilities; the enum stays authoritative for RLS. A sync trigger keeps them coherent during the Postgrest sunset:

- _New-path writes_ (GraphQL/Rust) set the array directly and project to the enum: `admin` if the array contains it, else `write`, else `read`. Orthogonal-only grants (e.g. `{billing}`) project to enum `read`, accepting a Postgrest read-leak within the prefix as Postgrest is sunsetting.
- _Legacy-path writes_ (Postgrest/direct SQL) trigger a DB function that expands the enum to its tier capabilities (`admin → {read, write, admin}`, `write → {read, write}`, `read → {read}`) and merges them with any existing orthogonal capabilities on the row. A Postgrest write re-expresses only the tier portion; capabilities like `billing` are left untouched. Postgrest can't remove orthogonal capabilities, which is fine — they're only managed through the new path.
- Add a `capabilities capability[]` column to `role_grants` (same as `user_grants`), backfill from the existing enum, and update role-grant traversal logic to compute intersections against the new array.
- A one-shot backfill populates tier capabilities into the array for all existing rows using the same expansion.
- If we decide to add the `traverse` capability, this backfill should also add `traverse` to every existing admin user and role grant, preserving today's behavior where admins can follow role-grant edges. Going forward, `traverse` is auto-bundled whenever an `admin` grant is created — the grant-expansion rule becomes `admin → {read, write, admin, traverse}`. A later phase of the user-management RFC will unbundle `traverse` from `admin` when the UI supports assigning capabilities individually.

**Phase 3 — cutover.** Once Postgrest retires, drop the enum column on both tables, remove the sync trigger, and remove the projection logic. `CapabilitySet` becomes the only representation. The publish-target check becomes a plain flag-containment test for `write`; admin grants continue to satisfy it because the grant-expansion rule always stores `{read, write, admin}` on admin grants.

**Phase 4 — rename and split the legacy tier names.** With Postgrest gone and `CapabilitySet` as the sole representation, the `write` and `admin` names can be replaced with capabilities that describe specific powers (e.g. `publish`, `manage`, or finer splits between task control and catalog edits). This is a pure rename/split inside the new model — a migration on `grant_capability` values, updates to the Rust `CapabilitySet` variants, and a sweep of the call sites. Sequenced last because it's disruptive to read without a forcing function, and only makes sense once nothing outside the new model speaks the old names.
