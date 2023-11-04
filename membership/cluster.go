package membership

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	kitlog "github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/sadath-12/keywave/internal/generic"
	"github.com/sadath-12/keywave/nodeapi"
)

var (
	_ Cluster = (*SWIMCluster)(nil)
)

type Cluster interface {
	Node(id NodeID) (Node, bool)
	Nodes() []Node
	SelfID() NodeID
	Self() Node

	ConnContext(ctx context.Context, id NodeID) (nodeapi.Client, error)
	Conn(id NodeID) (nodeapi.Client, error)
	LocalConn() nodeapi.Client

	ApplyState(nodes []Node, sourceID NodeID) []Node
	StateHash() uint64
}

type SWIMCluster struct {
	mut           sync.RWMutex
	selfID        NodeID
	stateHash     uint64
	nodes         map[NodeID]Node
	connections   map[NodeID]nodeapi.Client
	waiting       *generic.SyncMap[NodeID, chan struct{}]
	lastSync      map[NodeID]time.Time
	dialer        nodeapi.Dialer
	logger        kitlog.Logger
	dialTimeout   time.Duration
	probeTimeout  time.Duration
	probeInterval time.Duration
	probeJitter   time.Duration
	indirectNodes int
	gcInterval    time.Duration
	stop          chan struct{}
	wg            sync.WaitGroup
}

func withLock(l sync.Locker, f func()) {
	l.Lock()
	defer l.Unlock()
	f()
}

func NewSWIM(conf Config) *SWIMCluster {
	localNode := Node{
		ID:         conf.NodeID,
		Name:       conf.NodeName,
		PublicAddr: conf.PublicAddr,
		LocalAddr:  conf.LocalAddr,
		Status:     StatusHealthy,
		RunID:      time.Now().Unix(),
		Gen:        1,
	}

	logger := kitlog.With(conf.Logger, "package", "membership")
	nodes := make(map[NodeID]Node, 1)
	nodes[localNode.ID] = localNode

	return &SWIMCluster{
		nodes:         nodes,
		selfID:        localNode.ID,
		stateHash:     localNode.Hash64(),
		connections:   make(map[NodeID]nodeapi.Client),
		waiting:       new(generic.SyncMap[NodeID, chan struct{}]),
		lastSync:      make(map[NodeID]time.Time),
		dialer:        conf.Dialer,
		logger:        logger,
		probeTimeout:  conf.ProbeTimeout,
		probeInterval: conf.ProbeInterval,
		dialTimeout:   conf.DialTimeout,
		gcInterval:    conf.GCInterval,
		indirectNodes: conf.IndirectNodes,
		stop:          make(chan struct{}),
	}
}

// Start schedules background tasks for managing the cluster state, such as
// probing nodes and garbage collecting nodes that have left the cluster.
func (cl *SWIMCluster) Start() {
	cl.startDetector()
	cl.startGC()
}

// SelfID returns the ID of the current node.
func (cl *SWIMCluster) SelfID() NodeID {
	return cl.selfID
}

// Self returns the current node.
func (cl *SWIMCluster) Self() Node {
	cl.mut.RLock()
	defer cl.mut.RUnlock()

	return cl.nodes[cl.selfID]
}

// Nodes returns a list of all nodes in the cluster, including the current node,
// and nodes that have recently left the cluster but have not been garbage
// collected yet.
func (cl *SWIMCluster) Nodes() []Node {
	cl.mut.RLock()
	defer cl.mut.RUnlock()

	nodes := make([]Node, 0, len(cl.nodes))

	for _, node := range cl.nodes {
		nodes = append(nodes, node)
	}

	sort.Slice(nodes, func(i, j int) bool {
		return nodes[i].ID < nodes[j].ID
	})

	return nodes
}

// Node returns the node with the given ID, if it exists.
func (cl *SWIMCluster) Node(id NodeID) (Node, bool) {
	cl.mut.RLock()
	defer cl.mut.RUnlock()
	node, ok := cl.nodes[id]

	return node, ok
}

// Join adds the current node to the cluster with the given address.
// All nodes from the remote cluster are added to the local cluster and vice versa.
func (cl *SWIMCluster) Join(ctx context.Context, addr string) error {
	for _, node := range cl.Nodes() {
		if node.PublicAddr == addr {
			return nil // already joined
		}
	}

	conn, err := cl.dialer(ctx, addr)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	defer func() {
		if err := conn.Close(); err != nil {
			level.Warn(cl.logger).Log("msg", "failed to close connection", "node", addr, "err", err)
		}
	}()

	nodes , err := conn.PullPushState(ctx,toAPINodesInfo(cl.Nodes()))
	if err != nil {
		return fmt.Errorf("pull push state: %w", err)
	}
	cl.ApplyState(fromAPINodesInfo(nodes), 0)

	return nil
}
