import argparse
import json
import random

# Number of pool slots per TS.
POOL_SLOTS_PER_TS=10

# Maximum number of time steps it takes a move to complete.
MAX_MOVE_STEPS = 5

# Event types.
# Move succeeded. The payload will be (table name, (src TS index, dest TS index)).
EVT_MOVE_SUCCEED = "move_succeed"
EVT_MOVE_FAIL = "move_fail"

# Simulation stats class -- a collection for various metrics.
class EventStats:
    def __init__(self):
        self.event_count = {}
        self.event_count[EVT_MOVE_SUCCEED] = 0
        self.event_count[EVT_MOVE_FAIL] = 0

    def increment(self, evt_type):
        self.event_count[evt_type] += 1

    def get_stats(self, evt_type):
        return self.event_count[evt_type]

    def print_stats(self):
        for k, v in self.event_count.iteritems():
            print("{}\t\t{}".format(k, v))

# Read and parse cluster state from the file at the specified path. See
# the doc for the get_uniform_cluster() function for the cluster state format.
def parse_cluster_state(cluster_fpath):
    cluster = []
    f = open(cluster_fpath)
    for t in json.load(f):
        cluster.append(t)
    return cluster

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
def table_skew(table):
    return max(table) - min(table)

# Return the next move that should be done to balance table, encoded as (i, j)
# where i is the index of the TS to move from and j is the index of the TS to
# move to.
def pick_move(table):
    if not table:
        return None
    min_idx = 0
    max_idx = 0
    for i, e in enumerate(table):
        if e <= table[min_idx]:
            min_idx = i
        if e >= table[max_idx]:
            max_idx = i
    if min_idx == max_idx or table[max_idx] - table[min_idx] <= 1:
        return None
    return (max_idx, min_idx)

# Apply the move 'move' to the table 'table'.
def apply_move(op):
    table, move, _ = op
    src, dst = move
    table[src] -= 1
    table[dst] += 1

def revert_move(op):
    table, move, _ = op
    src, dst = move
    table[src] += 1
    table[dst] -= 1

# Apply the events due at the specified time.
def process_events(events, t, pool, stats):
    # Process special events first.
    processed_event_ids = []
    for event_id, event in events.iteritems():
        event_time = event[0]
        event_type = event[1]
        event_data = event[2]
        if event_type == EVT_MOVE_FAIL:
            if event_time == t:
                failure_fraction = event_data['fraction']
                if random.random() < failure_fraction and len(pool) > 0:
                    keys = pool.keys()
                    revert_op_id = keys[random.randrange(0, len(keys))]
                    op = pool[revert_op_id]
                    revert_move(op)
                    # Mark corresponding EVT_MOVE_SUCCEED event for removal.
                    processed_event_ids.append(op[2])
                    del pool[revert_op_id]
                    stats.increment(event_type)
                # EVT_MOVE_FAIL is a timespan event, self-perpetuating itself
                events[event_id] = (event_time + 1, event_type, event_data)
            if event_data['stop_time'] == t:
                processed_event_ids.append(event_id)
    for event_id in processed_event_ids:
        del events[event_id]
    processed_event_ids = []

    for event_id, event in events.iteritems():
        event_time = event[0]
        event_type = event[1]
        event_data = event[2]
        if event_type == EVT_MOVE_FAIL:
            # Should have been processed already.
            assert(t != event_time)
            continue
        elif event_type == EVT_MOVE_SUCCEED:
            if t == event_time:
                op_id = event_data[2]
                del pool[op_id]
                stats.increment(event_type)
                processed_event_ids.append(event_id)
        else:
            raise Exception("unknown event type: {}".format(event_type))
    for event_id in processed_event_ids:
        del events[event_id]


def has_standard_events(events):
    for _, event in events.iteritems():
        event_type = event[1]
        if event_type == EVT_MOVE_SUCCEED:
            return True
        elif event_type == EVT_MOVE_FAIL:
            continue
        else:
            raise Exception("unknown event type: {}".format(event_type))


# Parse the file with events to be injected during the simulation. The file
# format for the file is JSON, and the informal scheme is the following:
# [ { "time": <int>, "type": <str>, "data": <object> }, ... ]
#
# E.g.:
# [
#   {
#     "time": 10,
#     "type": "move_fail",
#     "data": { "fraction": 0.5, "stop_time": 1000 }
#   }
# ]
#
def parse_events(events_fpath, event_id):
    events = {}
    f = open(events_fpath)
    for e in json.load(f):
        event_id += 1
        events[event_id] = (e['time'], e['type'], e['data'])
    return events, event_id


def parse_args():
    parser = argparse.ArgumentParser(description="A simulation of Kudu rebalancing")
    parser.add_argument("--ts", type=int, default=10, help="the number of tablet servers")
    parser.add_argument("--tables", type=int, default=5, help="the number of tables")
    parser.add_argument("--replicas_per_ts_per_table", type=int, default=100,
                        help="maximum number of replicas per table and tablet server")
    parser.add_argument("--initial_cluster_state", type=str, default="",
                        help="path to the file containing intial state of the cluster; "
                        "if not set, the initial state is auto-generated")
    parser.add_argument("--injected_events", type=str, default="",
                        help="path to the file containing set of events to inject")
    args = parser.parse_args()
    return args.ts, args.tables, args.replicas_per_ts_per_table,\
            args.initial_cluster_state, args.injected_events

def main():
    # Set initial state of cluster
    n, t, r, initial_cluster_state_fpath, injected_events_fpath = parse_args()

    cluster = []
    if initial_cluster_state_fpath:
        cluster = parse_cluster_state(initial_cluster_state_fpath)
    else:
        cluster = gen_uniform_cluster(n, t, r)
    print "Initial cluster state =", cluster

    # Set up pool of fixed capacity
    max_pool_size = POOL_SLOTS_PER_TS * n
    pool = {}

    # Set up event queue.
    # An event will be modeled as a tuple (time, type, data needed to apply event to the cluster).
    events = {}

    # Assign unique identifiers to the events.
    event_id = -1

    # Add injected events, if any specified.
    if injected_events_fpath:
        events, event_id = parse_events(injected_events_fpath, event_id)
        print("injecting events: {}".format(events))

    # The total number of replicas shouldn't change. Compute it pre-rebalancing
    # so we can check that invariant.
    total_replicas = total_replicas_in_cluster(cluster)

    # Assign unique identifiers to the operations in the pool.
    op_id = -1

    # Advance time in discrete steps.
    t = -1

    stats = EventStats()
    has_moves = True
    while has_moves or has_standard_events(events) > 0:
        # Invariants for each time step.
        # Total number of replicas is constant.
        # TODO: Total number of replicas per table is constant.
        assert(total_replicas == total_replicas_in_cluster(cluster))
        # Every event is an ongoing move; True for now.
        #assert(len(pool) == len(events))

        # Advance time at the start so we can use continue statements.
        t += 1

        # Process regular events.
        process_events(events, t, pool, stats)

        # Each time step we exhaust our pool capacity.
        while len(pool) < max_pool_size:
            # Sort tables by skew, from highest to lowest skew, and pick
            # a table with the highest skew to operate with.
            table = sorted([table for table in cluster], key=table_skew, reverse=True)[0]

            # Pick the move.
            move = pick_move(table)
            has_moves = True if move else False;
            if not has_moves:
                break

            # Cheat a bit and just apply the move.
            # This avoids queueing it to succeed at some point and then having to apply it each
            # time step, which involves making copies of the cluster state to which the move can be applied.
            # Errors could be modeled as "undos" of moves in this paradigm.
            op_id += 1
            event_id += 1
            pool[op_id] = (table, move, event_id)
            apply_move(pool[op_id])

            # Add a no-op to the event queue for when the move succeeds.
            t_complete = t + random.randrange(1, MAX_MOVE_STEPS)
            events[event_id] = (t_complete, EVT_MOVE_SUCCEED, (table, move, op_id))

    print("BALANCED at t = {}".format(t))
    print("cluster = {}".format(cluster))
    stats.print_stats()

if __name__ == "__main__":
    main()
