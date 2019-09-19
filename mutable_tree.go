package iavl

import (
	"bytes"
	"fmt"
	"time"

	cmn "github.com/tendermint/tendermint/libs/common"
	dbm "github.com/tendermint/tendermint/libs/db"
)

const (
	maxVersions = 1000000
	maxNodes    = 500000
)

// ErrVersionDoesNotExist is returned if a requested version does not exist.
var ErrVersionDoesNotExist = fmt.Errorf("version does not exist")

// MutableTree is a persistent tree which keeps track of versions.
type MutableTree struct {
	*ImmutableTree                  // The current, working tree.
	lastSaved      *ImmutableTree   // The most recently saved tree.
	orphans        map[string]int64 // Nodes removed by changes to working tree.
	versions       map[int64]bool   // The previous, saved versions of the tree.
	ndb            *nodeDB
	nodeVersions   *NodeVersions
}

// NewMutableTree returns a new tree with the specified cache size and datastore.
func NewMutableTree(db dbm.DB, cacheSize int) *MutableTree {
	ndb := NewNodeDB(db, cacheSize)
	nodeVersions := NewNodeVersions(maxVersions, maxNodes, 0)
	head := &ImmutableTree{
		ndb:          ndb,
		nodeVersions: nodeVersions,
	}

	return &MutableTree{
		ImmutableTree: head,
		lastSaved:     head.clone(),
		orphans:       map[string]int64{},
		versions:      map[int64]bool{},
		ndb:           ndb,
		nodeVersions:  nodeVersions,
	}
}

// IsEmpty returns whether or not the tree has any keys. Only trees that are
// not empty can be saved.
func (tree *MutableTree) IsEmpty() bool {
	return tree.ImmutableTree.Size() == 0
}

// VersionExists returns whether or not a version exists.
func (tree *MutableTree) VersionExists(version int64) bool {
	return tree.versions[version]
}

// Hash returns the hash of the latest saved version of the tree, as returned
// by SaveVersion. If no versions have been saved, Hash returns nil.
func (tree *MutableTree) Hash() []byte {
	if tree.version > 0 {
		return tree.lastSaved.Hash()
	}
	return nil
}

// WorkingHash returns the hash of the current working tree.
func (tree *MutableTree) WorkingHash() []byte {
	return tree.ImmutableTree.Hash()
}

// String returns a string representation of the tree.
func (tree *MutableTree) String() string {
	return tree.ndb.String()
}

// Set/Remove will orphan at most tree.Height nodes,
// balancing the tree after a Set/Remove will orphan at most 3 nodes.
func (tree *MutableTree) prepareOrphansSlice() []*Node {
	return make([]*Node, 0, tree.Height()+3)
}

// Set sets a key in the working tree. Nil values are not supported.
func (tree *MutableTree) Set(key, value []byte) bool {
	orphaned, updated := tree.set(key, value)
	tree.addOrphans(orphaned)
	return updated
}

func (tree *MutableTree) set(key []byte, value []byte) (orphans []*Node, updated bool) {
	if value == nil {
		panic(fmt.Sprintf("Attempt to store nil value at key '%s'", key))
	}
	if tree.ImmutableTree.root == nil {
		tree.ImmutableTree.root = NewNode(key, value, tree.version+1)
		tree.nodeVersions.Inc1(tree.version + 1)
		return nil, false
	}
	orphans = tree.prepareOrphansSlice()
	tree.ImmutableTree.root, updated = tree.recursiveSet(tree.ImmutableTree.root, key, value, &orphans)
	return orphans, updated
}

func (tree *MutableTree) recursiveSet(node *Node, key []byte, value []byte, orphans *[]*Node) (
	newSelf *Node, updated bool,
) {
	version := tree.version + 1

	if node.isLeaf() {
		switch bytes.Compare(key, node.key) {
		case -1:
			tree.nodeVersions.Inc(version, 2)
			return &Node{
				key:       node.key,
				height:    1,
				size:      2,
				leftNode:  NewNode(key, value, version),
				rightNode: node,
				version:   version,
			}, false
		case 1:
			tree.nodeVersions.Inc(version, 2)
			return &Node{
				key:       key,
				height:    1,
				size:      2,
				leftNode:  node,
				rightNode: NewNode(key, value, version),
				version:   version,
			}, false
		default:
			*orphans = append(*orphans, node)
			tree.nodeVersions.Update(node.version, version)
			return NewNode(key, value, version), true
		}
	} else {
		*orphans = append(*orphans, node)
		tree.nodeVersions.Update(node.version, version)
		node = node.clone(version)

		if bytes.Compare(key, node.key) < 0 {
			node.leftNode, updated = tree.recursiveSet(node.getLeftNode(tree.ImmutableTree), key, value, orphans)
			node.leftHash = nil // leftHash is yet unknown
		} else {
			node.rightNode, updated = tree.recursiveSet(node.getRightNode(tree.ImmutableTree), key, value, orphans)
			node.rightHash = nil // rightHash is yet unknown
		}

		if updated {
			return node, updated
		}
		node.calcHeightAndSize(tree.ImmutableTree)
		newNode := tree.balance(node, orphans)
		return newNode, updated
	}
}

// Remove removes a key from the working tree.
func (tree *MutableTree) Remove(key []byte) ([]byte, bool) {
	val, orphaned, removed := tree.remove(key)
	tree.addOrphans(orphaned)
	return val, removed
}

// remove tries to remove a key from the tree and if removed, returns its
// value, nodes orphaned and 'true'.
func (tree *MutableTree) remove(key []byte) (value []byte, orphaned []*Node, removed bool) {
	if tree.root == nil {
		return nil, nil, false
	}
	orphaned = tree.prepareOrphansSlice()
	newRootHash, newRoot, _, value := tree.recursiveRemove(tree.root, key, &orphaned)
	if len(orphaned) == 0 {
		return nil, nil, false
	}

	if newRoot == nil && newRootHash != nil {
		tree.root = tree.ndb.GetNode(newRootHash)
	} else {
		tree.root = newRoot
	}
	return value, orphaned, true
}

// removes the node corresponding to the passed key and balances the tree.
// It returns:
// - the hash of the new node (or nil if the node is the one removed)
// - the node that replaces the orig. node after remove
// - new leftmost leaf key for tree after successfully removing 'key' if changed.
// - the removed value
// - the orphaned nodes.
func (tree *MutableTree) recursiveRemove(node *Node, key []byte, orphans *[]*Node) (newHash []byte, newSelf *Node, newKey []byte, newValue []byte) {
	version := tree.version + 1

	if node.isLeaf() {
		if bytes.Equal(key, node.key) {
			*orphans = append(*orphans, node)
			tree.nodeVersions.Dec1(node.version)
			return nil, nil, nil, node.value
		}
		return node.hash, node, nil, nil
	}

	// node.key < key; we go to the left to find the key:
	if bytes.Compare(key, node.key) < 0 {
		newLeftHash, newLeftNode, newKey, value := tree.recursiveRemove(node.getLeftNode(tree.ImmutableTree), key, orphans)

		if len(*orphans) == 0 {
			return node.hash, node, nil, value
		} else if newLeftHash == nil && newLeftNode == nil { // left node held value, was removed
			return node.rightHash, node.rightNode, node.key, value
		}
		*orphans = append(*orphans, node)
		tree.nodeVersions.Update(node.version, version)
		newNode := node.clone(version)
		newNode.leftHash, newNode.leftNode = newLeftHash, newLeftNode
		newNode.calcHeightAndSize(tree.ImmutableTree)
		newNode = tree.balance(newNode, orphans)

		return newNode.hash, newNode, newKey, value
	}
	// node.key >= key; either found or look to the right:
	newRightHash, newRightNode, newKey, value := tree.recursiveRemove(node.getRightNode(tree.ImmutableTree), key, orphans)

	if len(*orphans) == 0 {
		return node.hash, node, nil, value
	} else if newRightHash == nil && newRightNode == nil { // right node held value, was removed
		return node.leftHash, node.leftNode, nil, value
	}
	*orphans = append(*orphans, node)

	newNode := node.clone(version)
	newNode.rightHash, newNode.rightNode = newRightHash, newRightNode
	if newKey != nil {
		newNode.key = newKey
	}
	newNode.calcHeightAndSize(tree.ImmutableTree)
	newNode = tree.balance(newNode, orphans)
	return newNode.hash, newNode, nil, value
}

// Load the latest versioned tree from disk.
func (tree *MutableTree) Load() (int64, error) {
	return tree.LoadVersion(int64(0))
}

// SetVersion set current version of the tree. Only used in upgrade
func (tree *MutableTree) SetVersion(version int64) {
	tree.version = version
	tree.ndb.latestVersion = version
}

// Returns the version number of the latest version found
func (tree *MutableTree) LoadVersion(targetVersion int64) (int64, error) {
	roots, err := tree.ndb.getRoots()
	if err != nil {
		return 0, err
	}
	if len(roots) == 0 {
		return 0, nil
	}
	latestVersion := int64(0)
	var latestRoot []byte
	for version, r := range roots {
		tree.versions[version] = true
		if version > latestVersion &&
			(targetVersion == 0 || version <= targetVersion) {
			latestVersion = version
			latestRoot = r
		}
	}

	if !(targetVersion == 0 || latestVersion == targetVersion) {
		return latestVersion, fmt.Errorf("wanted to load target %v but only found up to %v",
			targetVersion, latestVersion)
	}

	nodeVersions := NewNodeVersions(maxVersions, maxNodes, latestVersion)
	t := &ImmutableTree{
		ndb:          tree.ndb,
		version:      latestVersion,
		nodeVersions: nodeVersions,
	}
	if len(latestRoot) != 0 {
		t.root = tree.ndb.GetNode(latestRoot)
		t.nodeVersions.Inc1(latestVersion)
	}

	tree.orphans = map[string]int64{}
	tree.ImmutableTree = t
	tree.lastSaved = t.clone()
	tree.nodeVersions = nodeVersions
	return latestVersion, nil
}

// LoadVersionOverwrite returns the version number of targetVersion.
// Higher versions' data will be deleted.
func (tree *MutableTree) LoadVersionForOverwriting(targetVersion int64) (int64, error) {
	latestVersion, err := tree.LoadVersion(targetVersion)
	if err != nil {
		return latestVersion, err
	}
	tree.deleteVersionsFrom(targetVersion + 1)
	return targetVersion, nil
}

// GetImmutable loads an ImmutableTree at a given version for querying
func (tree *MutableTree) GetImmutable(version int64) (*ImmutableTree, error) {
	rootHash := tree.ndb.getRoot(version)
	if rootHash == nil {
		return nil, ErrVersionDoesNotExist
	} else if len(rootHash) == 0 {
		return &ImmutableTree{
			ndb:          tree.ndb,
			version:      version,
			nodeVersions: tree.nodeVersions,
		}, nil
	}
	return &ImmutableTree{
		root:         tree.ndb.GetNode(rootHash),
		ndb:          tree.ndb,
		version:      version,
		nodeVersions: tree.nodeVersions,
	}, nil
}

// Rollback resets the working tree to the latest saved version, discarding
// any unsaved modifications.
func (tree *MutableTree) Rollback() {
	if tree.version > 0 {
		tree.ImmutableTree = tree.lastSaved.clone()
	} else {
		tree.ImmutableTree = &ImmutableTree{ndb: tree.ndb, version: 0}
	}
	tree.orphans = map[string]int64{}
}

// GetVersioned gets the value at the specified key and version.
func (tree *MutableTree) GetVersioned(key []byte, version int64) (
	index int64, value []byte,
) {
	if tree.versions[version] {
		t, err := tree.GetImmutable(version)
		if err != nil {
			return -1, nil
		}
		return t.Get(key)
	}
	return -1, nil
}

// SaveVersion saves a new tree version to disk, based on the current state of
// the tree. Returns the hash and new version number.
func (tree *MutableTree) SaveVersion() ([]byte, int64, error) {
	version := tree.version + 1

	if tree.versions[version] {
		//version already exists, throw an error if attempting to overwrite
		// Same hash means idempotent.  Return success.
		existingHash := tree.ndb.getRoot(version)
		var newHash = tree.WorkingHash()
		if bytes.Equal(existingHash, newHash) {
			tree.version = version
			tree.ImmutableTree = tree.ImmutableTree.clone()
			tree.lastSaved = tree.ImmutableTree.clone()
			tree.orphans = map[string]int64{}
			tree.nodeVersions = NewNodeVersions(maxVersions, maxNodes, version)
			return existingHash, version, nil
		}
		return nil, version, fmt.Errorf("version %d was already saved to different hash %X (existing hash %X)",
			version, newHash, existingHash)
	}

	if tree.root == nil {
		// There can still be orphans, for example if the root is the node being
		// removed.
		debug("SAVE EMPTY TREE %v\n", version)
		tree.ndb.SaveOrphans(version, tree.orphans)
		tree.ndb.SaveEmptyRoot(version, false)
	} else {
		debug("SAVE TREE %v\n", version)
		// Save the current tree.
		tree.ndb.SaveBranch(tree.root)
		tree.ndb.SaveOrphans(version, tree.orphans)
		tree.ndb.SaveRoot(tree.root, version, false)
	}
	tree.ndb.Commit()
	tree.version = version
	tree.versions[version] = true

	// Set new working tree.
	tree.ImmutableTree = tree.ImmutableTree.clone()
	tree.lastSaved = tree.ImmutableTree.clone()
	tree.orphans = map[string]int64{}
	maxPruneVersion, err := tree.nodeVersions.Commit(version)
	if err != nil {
		return nil, version, err
	}
	if maxPruneVersion > 0 {
		startPruneTime := time.Now()
		tree.PruneInMemory(maxPruneVersion)
		fmt.Println("version", version, "cost", time.Now().Sub(startPruneTime).Nanoseconds(), "ms")
	 } else {
	 	fmt.Println()
	}
	return tree.Hash(), version, nil
}

func (tree *MutableTree) PruneInMemory(maxPruneVersion int64) {
	var iter func(root *Node)
	iter = func(root *Node) {
		if root == nil {
			return
		}
		// root's version is the biggest in its branch.
		if left := root.leftNode; left != nil {
			if left.version <= maxPruneVersion {
				root.leftNode = nil
			} else {
				iter(left)
			}
		}
		if right := root.rightNode; right != nil {
			if right.version <= maxPruneVersion {
				root.rightNode = nil
			} else {
				iter(right)
			}
		}
	}
	// we do not check the root's version as it's always the newest version and won't be pruned.
	iter(tree.ImmutableTree.root)
}

// DeleteVersion deletes a tree version from disk. The version can then no
// longer be accessed.
func (tree *MutableTree) DeleteVersion(version int64) error {
	if version == 0 {
		return cmn.NewError("version must be greater than 0")
	}
	if version == tree.version {
		return cmn.NewError("cannot delete latest saved version (%d)", version)
	}
	if _, ok := tree.versions[version]; !ok {
		return cmn.ErrorWrap(ErrVersionDoesNotExist, "")
	}

	tree.ndb.DeleteVersion(version, true)
	tree.ndb.Commit()

	delete(tree.versions, version)

	return nil
}

// deleteVersionsFrom deletes tree version from disk specified version to latest version. The version can then no
// longer be accessed.
func (tree *MutableTree) deleteVersionsFrom(version int64) error {
	if version <= 0 {
		return cmn.NewError("version must be greater than 0")
	}
	newLatestVersion := version - 1
	lastestVersion := tree.ndb.getLatestVersion()
	for ; version <= lastestVersion; version++ {
		if version == tree.version {
			return cmn.NewError("cannot delete latest saved version (%d)", version)
		}
		if _, ok := tree.versions[version]; !ok {
			return cmn.ErrorWrap(ErrVersionDoesNotExist, "")
		}
		tree.ndb.DeleteVersion(version, false)
		delete(tree.versions, version)
	}
	tree.ndb.Commit()
	tree.ndb.resetLatestVersion(newLatestVersion)
	return nil
}

// Rotate right and return the new node and orphan.
func (tree *MutableTree) rotateRight(node *Node, orphans *[]*Node) *Node {
	version := tree.version + 1

	// TODO: optimize balance & rotate.
	node = node.clone(version)
	orphaned := node.getLeftNode(tree.ImmutableTree)
	*orphans = append(*orphans, orphaned)
	newNode := orphaned.clone(version)
	tree.nodeVersions.Update(orphaned.version, version)

	newNoderHash, newNoderCached := newNode.rightHash, newNode.rightNode
	newNode.rightHash, newNode.rightNode = node.hash, node
	node.leftHash, node.leftNode = newNoderHash, newNoderCached

	node.calcHeightAndSize(tree.ImmutableTree)
	newNode.calcHeightAndSize(tree.ImmutableTree)

	return newNode
}

// Rotate left and return the new node and orphan.
func (tree *MutableTree) rotateLeft(node *Node, orphans *[]*Node) *Node {
	version := tree.version + 1

	// TODO: optimize balance & rotate.
	node = node.clone(version)
	orphaned := node.getRightNode(tree.ImmutableTree)
	*orphans = append(*orphans, orphaned)
	newNode := orphaned.clone(version)
	tree.nodeVersions.Update(orphaned.version, version)

	newNodelHash, newNodelCached := newNode.leftHash, newNode.leftNode
	newNode.leftHash, newNode.leftNode = node.hash, node
	node.rightHash, node.rightNode = newNodelHash, newNodelCached

	node.calcHeightAndSize(tree.ImmutableTree)
	newNode.calcHeightAndSize(tree.ImmutableTree)

	return newNode
}

// NOTE: assumes that node can be modified
// TODO: optimize balance & rotate
func (tree *MutableTree) balance(node *Node, orphans *[]*Node) (newSelf *Node) {
	if node.persisted {
		panic("Unexpected balance() call on persisted node")
	}
	balance := node.calcBalance(tree.ImmutableTree)

	if balance > 1 {
		if node.getLeftNode(tree.ImmutableTree).calcBalance(tree.ImmutableTree) >= 0 {
			// Left Left Case
			newNode := tree.rotateRight(node, orphans)
			return newNode
		}
		// Left Right Case
		left := node.getLeftNode(tree.ImmutableTree)
		*orphans = append(*orphans, left)
		tree.nodeVersions.Dec1(left.version)
		node.leftHash = nil
		node.leftNode = tree.rotateLeft(left, orphans)
		newNode := tree.rotateRight(node, orphans)
		return newNode
	}
	if balance < -1 {
		if node.getRightNode(tree.ImmutableTree).calcBalance(tree.ImmutableTree) <= 0 {
			// Right Right Case
			newNode := tree.rotateLeft(node, orphans)
			return newNode
		}
		// Right Left Case
		right := node.getRightNode(tree.ImmutableTree)
		*orphans = append(*orphans, right)
		tree.nodeVersions.Dec1(right.version)
		node.rightHash = nil
		node.rightNode = tree.rotateRight(right, orphans)
		newNode := tree.rotateLeft(node, orphans)
		return newNode
	}
	// Nothing changed
	return node
}

func (tree *MutableTree) addOrphans(orphans []*Node) {
	for _, node := range orphans {
		if !node.persisted {
			// We don't need to orphan nodes that were never persisted.
			continue
		}
		if len(node.hash) == 0 {
			panic("Expected to find node hash, but was empty")
		}
		tree.orphans[string(node.hash)] = node.version
	}
}
