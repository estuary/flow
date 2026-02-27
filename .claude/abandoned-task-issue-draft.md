We have a bunch of abandoned tasks [^1]. We want to figure out a way to deal with them. In order to do that, we need to:
* Identify when a task is abandoned
* Notify the owner of the task that it's abandoned, and give them some time to remedy the situation
* Disable the abandoned task

**Out of scope** for this piece of work are tasks that are running but haven't moved any data, dealing with the collections of abandoned tasks, and subsequently deleting the disabled abandoned tasks.

## Proposed UX

1. A user publishes a database capture on day 1 of their POC
2. On day 3, they change the firewall rules so that database is no longer reachable
3. The task starts failing, and the control-plane diligently restarts it according to the task failure backoff strategy.
4. On day 3+N (14 maybe?), they get an email saying something like "Your task has been failing for N days. We've tried restarting it Q times without success. It will be disabled in M (maybe 7?) days from now. <links to troubleshooting or whatever>"
5. On day 3+N+M, the control-plane will create a publication that disables that task.
## Definition

A task counts as "abandoned" when:
- It's enabled
- It is a type of entity that has shards (captures, materializations, derivations)
  - **Note:** This excludes all Dekaf materializations, which is fine because they shouldn't be incurring the same kind of control-plane overhead as actual abandoned tasks do anyway.
- No shard has been PRIMARY since a configurable threshold of days ago. When PRIMARY has _never_ been observed, `created_at` is used as the fallback, so new tasks aren't flagged until they've existed for the full threshold.

## Approach

We'll use the controller-evaluated alert path (same as `ShardFailed`, `AutoDiscoverFailed`, `BackgroundPublicationFailed`). The per-spec controller already does periodic shard listings, so we already have most of the data we need.

One more piece of data is needed:

- `last_primary_ts: Option<DateTime<Utc>>` on `ActivationStatus`. Set to `now()`[^2] whenever `aggregate_shard_status()` returns `Ok` during a health check. No new data-plane calls, this piggy-backs on the existing `list_task_shards()`. Starts as `None` for existing tasks and updates with the normal health-check cadence.

Then, we'll add a new `evaluate_abandoned()` function and wire it into the capture, materialization, and collection codepaths after the activation stage. The controller runs the full pipeline on every wake, and broken tasks still wake regularly as e.g shard health checks back off to ~60min [^3]. Derivations are collections, so they're covered by the collection pipeline. We'll gate with `has_task_shards()` which returns `false` for plain collections, Dekaf tasks, disabled tasks etc. I'll call `set_alert_firing(TaskAbandoned, ...)` or `resolve_alert(TaskAbandoned)` based on whether the task meets the criteria above.

**Concept:** Would it also be worth tracking the number of times the controller has restarted a failing task, since it was last `PRIMARY`? That way we could include in the email some text like "We have attempted to restart your task 826 times since it started failing 14 days ago.". This would also be a useful metric to have in general.

## Rollout

We want to roll this out in stages.

**Detection only.** Fire alerts into `alert_history` but don't notify anyone. Don't add `task_abandoned` to default `include_alert_types` in `alert_subscriptions`. Deploy with a generous threshold (30 days) and let `last_primary_ts` accumulate. Monitor `alert_history` for `task_abandoned` entries and manually review what gets flagged.

**Notification.** Once we're confident the detection is flagging the right tasks, add `task_abandoned` to default `include_alert_types`, and probably run a migration to opt existing tenants in as well. By this point we'll need a real email template describing what "abandoned" means and what the user should do about it.

**Auto-disable.** When `TaskAbandoned` has been firing for longer than `ABANDONED_TASK_DISABLE_AFTER` (default 7 days), the controller creates a background publication setting `shards.disable = true`.

- On success, the task is disabled, `is_enabled_task()` returns false on the next run, the alert resolves, and the controller stops scheduling health checks.
  - **Note:** Make sure this doesn't send a "your task is no longer abandoned" alert resolution email when we disable lol.
- On failure, the alert stays firing and the controller retries on its next wake.

## (Un)answered questions

**Data-plane outage**: if the data-plane that a task is running on goes down, what happens? Will the controller handle not being able to make shard listings? Will the controller handle not being able to publish the disablement? I imagine as designed, we'll sit there trying over and over to disable these tasks.
  * Answer from talking with @psFried: yep, we don't have a good story around data-plane deletions. currently this will get stuck in a loop trying to activate the disablement or deletion unless support manually removes all tasks from a data-plane before removing it.

**Existing abandoned tasks:** As designed, this will apply to both existing and new abandoned tasks, after the threshold window has passed. That's probably what we want, just calling it out

**How do we want to communicate:** Do we want an email after X number of days without `PRIMARY`, then disable after Y? Multiple emails? Do we want a resolution email? etc

**Windows:** What are we thinking WRT the thresholds here? 14 days to warning email, then another week until disable?

[^1]: How many?
[^2]: Is `now()` correct? Do we have some other concept of time to worry about here?
———
**Answer:** The controller uses `control_plane.current_time()` rather than raw `now()`. In production this is just `Utc::now()`, but the indirection exists so that integration tests can use a controllable clock.
[^3]: Is this correct? I want to make sure I/we understand how often this automation will run. I feel like it doesn't _need_ to run more than roughly once per day, but more often also shouldn't hurt.
———
**Answer:** the controller picks the soonest of the reported wake-at times from the sub-tasks, so `evaluate_abandoned()` will just report whatever interval it cares about, like 24h, and that will guarantee that it gets woken no later than that.