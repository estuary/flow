# Learnings

## 2026-02-02 | Documentation | Internal links break when moving files

**Issue:** After moving docs from `features/` to `guides/advanced-usage/`, internal links using absolute paths (`/features/feature-flags/`) broke.

**Solution:** Use relative paths (`./feature-flags.md`) for links between files in the same directory. The Docusaurus build will catch broken links, but CI may catch them before local builds do.

---

## 2026-02-02 | Documentation | LLM-friendly spec documentation

**Issue:** Documentation showing YAML examples may not be clear enough for LLMs to programmatically modify specs.

**Solution:** Include explicit JSON paths like `materializations.<name>.endpoint.connector.config.advanced.feature_flags` and show "before/after" examples for modifications (e.g., appending to comma-separated strings).

---

## 2026-02-02 | Git | Cherry-picking to new branch workflow

**Issue:** Moving commits from one branch to a clean branch based on master requires multiple steps.

**Solution:**
```bash
git stash -u                          # Save uncommitted work
git checkout master && git pull       # Update master
git branch -D <branch>                # Delete old local branch if exists
git checkout -b <branch>              # Create fresh from master
git cherry-pick <commit1> <commit2>   # Cherry-pick commits
git stash pop                         # Restore uncommitted work
git push -u origin <branch> --force   # Push (force if remote exists)
```

---

## 2026-02-06 | Tooling | Gitleaks pre-commit hook with temp dirs breaks .gitleaksignore

**Issue:** Global pre-commit hook copied staged files to a temp dir then ran `gitleaks detect --no-git --source "$TMPDIR"`. File paths in the temp dir don't match .gitleaksignore fingerprints, so allowlisted files still get flagged.

**Solution:** Use `gitleaks protect --staged` instead. It reads directly from git's index with real file paths, so .gitleaksignore fingerprints work correctly. Much simpler hook too.

---

## 2026-02-06 | Estuary Docs | Terminology standards

**Issue:** PR review caught inconsistent terminology: "Flow" vs "Estuary", "materializers" vs "materialization connectors".

**Solution:** Product name is standardized to "Estuary" (not "Estuary Flow" or "Flow"). Use "materialization connectors" not "materializers". Web UI instructions should come before YAML in docs (UI-first convention).

---

## 2026-02-06 | Estuary | include vs require in field selection

**Issue:** Assumed `include` and `require` had different behavior in materialization field selection.

**Solution:** They are aliases and behave identically. Both require the field to exist in the collection schema. Neither controls nullability (that's determined by the collection schema). Standardize on `include` in docs since `require` appears nowhere else.

---

## 2026-02-02 | Docusaurus | Pre-existing build errors

**Issue:** The docs site has a pre-existing HubSpot redirect build error that causes `npm run build` to fail in the post-build phase.

**Solution:** The compilation succeeds (Client/Server compile successfully) - the error is in the redirect plugin. This is unrelated to content changes and can be ignored for content verification.
