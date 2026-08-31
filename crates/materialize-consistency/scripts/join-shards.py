#!/usr/bin/env python3
"""Rewrite a `gazctl shards list -o yaml` tree so applying it joins the task's shards
pairwise.

Each pair merges into the shard with the *lower* range, which absorbs its partner's and
then covers both; the partner is deleted. The survivor's range *begin* does not move, and
a shard's ID derives from that, so it keeps its ID, its recovery log and its accumulated
state. An odd shard out is left alone.

One detail matters for correctness: `gazctl` hoists labels common to every shard into a
`common:` block, so a range label can live there rather than on the shard. This flattens
them onto each shard before editing — otherwise widening one shard's range would silently
rewrite the hoisted value for all of them.
"""

import copy
import sys

import yaml

RANGE_LABELS = (
    "estuary.dev/key-begin",
    "estuary.dev/key-end",
    "estuary.dev/rclock-begin",
    "estuary.dev/rclock-end",
)


def labels_to_dict(labels):
    return {entry["name"]: entry.get("value", "") for entry in labels or []}


def dict_to_labels(mapping):
    return [{"name": name, "value": value} for name, value in sorted(mapping.items())]


def flatten(tree):
    """Return (common_without_labels, [(shard, effective_labels)])."""
    common = copy.deepcopy(tree.get("common") or {})
    common_labels = labels_to_dict(common.pop("labels", None))

    out = []
    for shard in tree.get("shards") or []:
        shard = copy.deepcopy(shard)
        effective = dict(common_labels)
        effective.update(labels_to_dict(shard.pop("labels", None)))
        out.append((shard, effective))
    return common, out


def range_of(labels):
    try:
        return tuple(int(labels[name], 16) for name in RANGE_LABELS)
    except KeyError as err:
        raise SystemExit(f"shard is missing the range label {err}")


def merge(lhs_labels, rhs_labels, lhs_id, rhs_id):
    """Widen `lhs_labels` to cover `rhs_labels`, on whichever single axis is adjacent."""
    lkb, lke, lrb, lre = range_of(lhs_labels)
    rkb, rke, rrb, rre = range_of(rhs_labels)

    if (lrb, lre) == (rrb, rre) and lke + 1 == rkb:
        lhs_labels["estuary.dev/key-end"] = f"{rke:08x}"
        return
    if (lkb, lke) == (rkb, rke) and lre + 1 == rrb:
        lhs_labels["estuary.dev/rclock-end"] = f"{rre:08x}"
        return

    raise SystemExit(
        f"shards {lhs_id} and {rhs_id} are not adjacent on a single axis and cannot be "
        f"joined (keys [{lkb:08x}, {lke:08x}] and [{rkb:08x}, {rke:08x}]; "
        f"r-clocks [{lrb:08x}, {lre:08x}] and [{rrb:08x}, {rre:08x}])"
    )


def main(src, dst):
    with open(src) as f:
        tree = yaml.safe_load(f)

    common, shards = flatten(tree)

    if len(shards) < 2:
        raise SystemExit(f"task has {len(shards)} shard(s), so there is nothing to join")

    # Sorted so a split's two children are neighbours.
    shards.sort(key=lambda pair: (range_of(pair[1])[2], range_of(pair[1])[0]))

    out = []
    for i in range(0, len(shards) - 1, 2):
        (lhs, lhs_labels), (rhs, rhs_labels) = shards[i], shards[i + 1]

        merge(lhs_labels, rhs_labels, lhs["id"], rhs["id"])
        lhs["labels"] = dict_to_labels(lhs_labels)
        out.append(lhs)

        # Only `id` and `revision` are needed to delete, and sending the rest would
        # invite a mismatch on a spec we are discarding anyway.
        out.append({"id": rhs["id"], "revision": rhs["revision"], "delete": True})
        print(f"Joining {rhs['id']} into {lhs['id']}.", file=sys.stderr)

    if len(shards) % 2 == 1:
        odd, odd_labels = shards[-1]
        odd["labels"] = dict_to_labels(odd_labels)
        out.append(odd)
        print(f"Leaving {odd['id']} unpaired.", file=sys.stderr)

    with open(dst, "w") as f:
        yaml.safe_dump({"common": common, "shards": out}, f, sort_keys=False)


if __name__ == "__main__":
    if len(sys.argv) != 3:
        raise SystemExit("usage: join-shards.py <listed.yaml> <out.yaml>")
    main(sys.argv[1], sys.argv[2])
