import argparse
import json
import random

# Number of pool slots per TS.
POOL_SLOTS_PER_TS=10

# Maximum number of time steps it takes a move to complete.
MAX_MOVE_STEPS = 5

# Event types.
EVT_MOVE_FAIL_FRACTION = "move_fail_fraction"
# Failed to move tablet replica.
EVT_MOVE_FAIL = "move_fail"
# Move succeeded. The payload will be (table name, (src TS index, dest TS index)).
EVT_MOVE_SUCCEED = "move_succeed"
# A new tablet server has been added into the system.
EVT_TSERVER_ADDED = "tserver_added"
# A new table has been created.
EVT_TABLE_CREATED = "table_created"
# A table has been dropped.
EVT_TABLE_DROPPED = "table_dropped"

# Simulation stats class -- a collection for various metrics.
class EventStats:
    def __init__(self):
        self.event_count = {}
        self.event_count[EVT_MOVE_SUCCEED] = 0
        self.event_count[EVT_MOVE_FAIL] = 0
        self.event_count[EVT_TABLE_CREATED] = 0
        self.event_count[EVT_TABLE_DROPPED] = 0

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


# Same as pick_move(), but also try to optimize both per-table and per-cluster
# skew.
def pick_move_cluster_balanced(table, cluster):
    if not table or not cluster:
        return None

    # TODO(aserbin): It's not quite optimal to recompute this every time; maybe
    #                it's worth to update the server_replicas_num every time
    #                a move happens and don't recompute everything from scratch.
    server_replicas_num = [0] * len(cluster[0])
    for t in cluster:
        for idx, replicas_num in enumerate(t):
            server_replicas_num[idx] += replicas_num

    min_idx = 0
    max_idx = 0
    for i, e in enumerate(table):
        if e <= table[min_idx]:
            if e < table[min_idx] or server_replicas_num[i] < server_replicas_num[min_idx]:
                min_idx = i
        if e >= table[max_idx]:
            if e > table[max_idx] or server_replicas_num[i] > server_replicas_num[max_idx]:
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
def process_events(cluster, events, t, pool, stats):
    move_failure_fraction = 0.0
    # Process special events first.
    processed_event_ids = []
    for event_id, event in events.iteritems():
        event_time = event[0]
        event_type = event[1]
        event_data = event[2]
        if event_time != t:
            continue

        if event_type == EVT_MOVE_FAIL_FRACTION:
            fraction = event_data['fraction']
            # EVT_MOVE_FAIL_FRACTION is a timespan event, self-perpetuating itself
            move_failure_fraction += fraction
            events[event_id] = (event_time + 1, event_type, event_data)
            if event_data['stop_time'] == t:
                move_failure_fraction -= fraction
                processed_event_ids.append(event_id)
        elif event_type == EVT_TSERVER_ADDED:
            # New tablet servers don't have any replicas at the beginning.
            for table in cluster:
                table.append(0)
            processed_event_ids.append(event_id)
        elif event_type == EVT_TABLE_CREATED:
            new_table = []
            last_table = cluster[-1]
            for e in last_table:
                new_table.append(e)
            cluster.append(new_table)
            stats.increment(event_type)
            processed_event_ids.append(event_id)
        elif event_type == EVT_TABLE_DROPPED:
            idx = random.randrange(0, len(cluster))
            del cluster[idx]
            stats.increment(event_type)
            processed_event_ids.append(event_id)

    for event_id in processed_event_ids:
        del events[event_id]
    processed_event_ids = []

    for event_id, event in events.iteritems():
        event_time = event[0]
        event_type = event[1]
        event_data = event[2]
        if event_type not in [ EVT_MOVE_SUCCEED, EVT_MOVE_FAIL ]:
            # Special events should have been processed already.
            assert(t != event_time)
            continue

        if event_type in [ EVT_MOVE_SUCCEED, EVT_MOVE_FAIL ]:
            if t == event_time:
                op_id = event_data[2]
                if event_type == EVT_MOVE_FAIL:
                    revert_move(pool[op_id])
                del pool[op_id]
                stats.increment(event_type)
                processed_event_ids.append(event_id)
        else:
            raise Exception("unknown event type: {}".format(event_type))

    for event_id in processed_event_ids:
        del events[event_id]

    return move_failure_fraction


def has_standard_events(events):
    for _, event in events.iteritems():
        event_type = event[1]
        if event_type in [ EVT_MOVE_SUCCEED, EVT_MOVE_FAIL ]:
            return True
        else:
            return False

# Compute and return per-table skew in the cluster.
def per_table_skew(cluster):
    if not cluster:
        return -1

    per_table_skew = []
    for table in cluster:
        per_table_skew.append(max(table) - min(table))
    return per_table_skew

# Compute and return cluster skew.
def cluster_skew(cluster):
    if not cluster:
        return -1

    per_server_replicas_num = [0] * len(cluster[-1])
    for table in cluster:
        for idx, replicas_num in enumerate(table):
            per_server_replicas_num[idx] += replicas_num
    min_replicas_num = min(per_server_replicas_num)
    max_replicas_num = max(per_server_replicas_num)

    return max_replicas_num - min_replicas_num


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
def parse_events(events_fpaths, events, event_id):
    fpaths = events_fpaths.split(",")
    for fpath in fpaths:
        f = open(fpath)
        for e in json.load(f):
            event_id += 1
            events[event_id] = (e['time'], e['type'], e['data'])
    return event_id


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
    n, t, r, initial_cluster_state_fpath, injected_events = parse_args()

    cluster = []
    if initial_cluster_state_fpath:
        cluster = parse_cluster_state(initial_cluster_state_fpath)
    else:
        cluster = gen_uniform_cluster(n, t, r)
    print("\ninitial cluster state\n{}\n".format(cluster))

    # Set up pool of fixed capacity
    max_pool_size = POOL_SLOTS_PER_TS * n
    pool = {}

    # Set up event queue.
    # An event will be modeled as a tuple (time, type, data needed to apply event to the cluster).
    events = {}

    # Assign unique identifiers to the events.
    event_id = -1

    # Add injected events, if any specified.
    if injected_events:
        event_id = parse_events(injected_events, events, event_id)
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
    # Continue while there are some moves to be performed or there are events
    # to happen.
    while has_moves or has_standard_events(events) > 0:
        # Advance time at the start so we can use continue statements.
        t += 1

        # Process regular events.
        move_failure_fraction = process_events(cluster, events, t, pool, stats)

        # Each time step we exhaust our pool capacity.
        while len(pool) < max_pool_size:
            # Sort tables by skew, from highest to lowest skew, and pick
            # a table with the highest skew to operate with.
            table = sorted([table for table in cluster], key=table_skew, reverse=True)[0]

            # Pick the move.
            #move = pick_move(table)
            move = pick_move_cluster_balanced(table, cluster)
            has_moves = True if move else False
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

            # Add an event for when move to succeed (no-op) or fail.
            t_complete = t + random.randrange(1, MAX_MOVE_STEPS)
            if random.random() < move_failure_fraction:
                events[event_id] = (t_complete, EVT_MOVE_FAIL, (table, move, op_id))
            else:
                events[event_id] = (t_complete, EVT_MOVE_SUCCEED, (table, move, op_id))

    print("BALANCED at t = {}".format(t))
    print("\nbalanced cluster state\n{}\n".format(cluster))
    print("per-table skew:\t{}".format(per_table_skew(cluster)))
    print("cluster skew  :\t{}".format(cluster_skew(cluster)))
    stats.print_stats()


if __name__ == "__main__":
    main()
