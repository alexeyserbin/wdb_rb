import argparse
import json
import random

# Number of pool slots per TS.
POOL_SLOTS_PER_TS=10

# Maximum number of time steps it takes a move to complete.
MAX_MOVE_STEPS = 5

# Event types.
# Move succeeded. The payload will be (table name, (src TS index, dest TS index)).
MOVE_SUCCEED = "move_succeed"
MOVE_FAIL_START = "move_fail_start"
MOVE_FAIL_STOP = "move_fail_stop"

# Generate a cluster of n tablet servers and t tables.
# Each tablet server will have will have < r replicas
# for each table, chosen uniformly at random from [0, r).
# A cluster is represented as a list of tables.
# a table is represented as a list of replica counts, one per ts.
# For example, a possible output of gen_uniform_cluster(5, 3, 10)
# is
# [[4, 8, 0, 6, 4],
#  [5, 9, 1, 3, 3],
#  [3, 7, 2, 0, 8]]
def gen_uniform_cluster(n, t, r):
    return [[random.randrange(r) for _ in range(n)] for i in range(t)]

def total_replicas_in_cluster(cluster):
    return reduce(lambda x, y: x + y, map(sum, cluster))

# Return the table skew of 'table'.
# Table skew is (# replicas on TS with most replica) - (# replicas on TS with least replica).
# This modifies table so it is sorted by # of replicas, increasing.
def table_skew(table):
    table.sort()
    return table[-1] - table[0]

# Return the next move that should be done to balance table, encoded as (i, j)
# where i is the index of the TS to move from and j is the index of the TS to
# move to.
# This function assumes the table isn't balanced, else it may return a useless or pernicious move.
# No randomization is added.
def pick_move(table):
    return (len(table) - 1, 0)

# Parse the file with events to be injected during the simulation. The file
# format for the file is JSON, and the informal scheme is the following:
# [ { "time": <int>, "type": <str>, "data": <object> }, ... ]
#
# E.g.:
# [
#   { "time": 10, "type": "move_fail_start", "data": { "fraction": 0.5 } },
#   { "time": 1000, "type": "move_fail_stop", "data": null }
# ]
#
def parse_events(events_fpath):
    events = []
    f = open(events_fpath)
    for e in json.load(f):
        events.append((e['time'], e['type'], e['data']))
    return events

# Apply the move 'move' to the table 'table'.
def apply_move(table, move):
    src, dst = move
    table[src] -= 1
    table[dst] += 1

# Apply the event to the cluster state.
# Nothing to do here right now since moves always succeed, so we apply them to the cluster state
# when they are proposed. This makes sense since we would consider them applied while they are in flight.
def apply_event(cluster, event):
    if event[1] == MOVE_FAIL_START:
        apply_event.failure_fraction = event[2]['fraction']
    if event[1] == MOVE_FAIL_STOP:
        apply_event.failure_fraction = 0

    if event[1] == MOVE_SUCCEED and apply_event.failure_fraction != 0:
        # Undo the move if it's set to fail.
        table = event[2][0]
        src, dst = event[2][1]
        if random.random() < apply_event.failure_fraction:
            table[src] += 1
            table[dst] -= 1
apply_event.failure_fraction = 0


def parse_args():
    parser = argparse.ArgumentParser(description="A simulation of Kudu rebalancing")
    parser.add_argument("--ts", type=int, default=10, help="the number of tablet servers")
    parser.add_argument("--tables", type=int, default=5, help="the number of tables")
    parser.add_argument("--replicas_per_ts_per_table", type=int, default=100,
                        help="maximum number of replicas per table and tablet server")
    parser.add_argument("--injected_events_fpath", type=str, default="",
                        help="path to the file containing set of events to inject")
    args = parser.parse_args()
    return args.ts, args.tables, args.replicas_per_ts_per_table, args.injected_events_fpath

def main():
    # Set initial state of cluster
    n, t, r, injected_events_fpath = parse_args()
    cluster = gen_uniform_cluster(n, t, r)
    print "Initial cluster state =", cluster

    # Set up pool of fixed capacity
    max_pool_size = POOL_SLOTS_PER_TS * n
    pool = []

    # Set up event queue.
    # An event will be modeled as a tuple (time, type, data needed to apply event to the cluster).
    events = []

    # Add injected events, if any specified.
    injected_events = []
    if injected_events_fpath:
        injected_events = parse_events(injected_events_fpath)
        print("injecting events: {}".format(injected_events))

    # The total number of replicas shouldn't change. Compute it pre-rebalancing
    # so we can check that invariant.
    # TODO: Stop sorting the cluster list so we can track table identity and maintain the invariant per table.
    total_replicas = total_replicas_in_cluster(cluster)

    # Advance time in discrete steps.
    t = -1
    while True:
        # Invariants for each time step.
        # Total number of replicas is constant.
        # TODO: Total number of replicas per table is constant.
        assert(total_replicas == total_replicas_in_cluster(cluster))
        # Every event is an ongoing move; True for now.
        assert(len(pool) == len(events))

        # Advance time at the start so we can use continue statements.
        t += 1

        for i, e in enumerate(injected_events):
            if e[0] == t:
                apply_event(cluster, e)
                injected_events.pop(i)

        #print ("Cluster state at t = %d:" % t), cluster

        # Apply events.
        # This is inefficient. Would be better to keep events sorted by time.
        # Also I'm cheating and I know I can pop a move off of the front of pool every time an event is triggered.
        for i, e in enumerate(events):
            if e[0] == t:
                apply_event(cluster, e)
                events.pop(i)
                pool.pop(0)

        # Don't act if the pool is full.
        if len(pool) >= max_pool_size:
            assert(len(pool) == max_pool_size)
            #print "pool full: no action for step t =", t
            continue

        # Each time step we exhaust our pool capacity.
        while len(pool) < max_pool_size:
            # Sort tables by skew, from highest to lowest skew.
            tables_by_skew = sorted([table for table in cluster], key=table_skew, reverse=True)

            # If the most skewed table is balanced, we're done.
            # Well, just because moves always succeed in this toy first version.
            # Really we should wait until the pool clears.
            if table_skew(tables_by_skew[0]) <= 1:
                print 'BALANCED at t =', t
                print "Cluster =", cluster
                exit(0)

            # Iterate by skew.
            for table in tables_by_skew:
                if len(pool) >= max_pool_size:
                    assert(len(pool) == max_pool_size)
                    #print "pool filled: not finding more moves for t =", t
                    break

                # Pick the move.
                move = pick_move(table)

                # Cheat a bit and just apply the move.
                # This avoids queueing it to succeed at some point and then having to apply it each
                # time step, which involves making copies of the cluster state to which the move can be applied.
                # Errors could be modeled as "undos" of moves in this paradigm.
                pool.append(move)
                apply_move(table, move)

                # Add a no-op to the event queue for when the move succeeds.
                events.append((t + random.randrange(1, MAX_MOVE_STEPS), MOVE_SUCCEED, (table, move)))

if __name__ == "__main__":
    main()
