// Package merkle implements Mini-Verde's job-bound RFC 6962-shaped Merkle
// commitments and inclusion proofs.
package merkle

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"math/bits"
)

const rootDomain = "mini-verde/root/v1"

var (
	ErrEmptyTree         = errors.New("merkle tree is empty")
	ErrLeafCountMismatch = errors.New("merkle leaf count mismatch")
	ErrIndexOutOfRange   = errors.New("merkle leaf index out of range")
)

// Digest is a SHA-256 digest.
type Digest = [sha256.Size]byte

// Proof is a leaf-to-root inclusion path. The verifier obtains the expected
// leaf count and job ID from trusted job context, not from this value.
type Proof struct {
	Index    uint64
	Siblings []Digest
}

// Tree is an immutable Merkle commitment for one job and leaf count.
type Tree struct {
	jobID     Digest
	leafCount uint64
	root      Digest
	treeRoot  *node
}

type node struct {
	hash      Digest
	leafCount uint64
	left      *node
	right     *node
}

// New constructs a tree over stateHashes. expectedLeafCount must come from
// the trusted job specification and must equal len(stateHashes).
func New(jobID Digest, expectedLeafCount uint64, stateHashes []Digest) (*Tree, error) {
	if expectedLeafCount == 0 {
		return nil, ErrEmptyTree
	}
	if uint64(len(stateHashes)) != expectedLeafCount {
		return nil, fmt.Errorf("%w: got %d, want %d", ErrLeafCountMismatch, len(stateHashes), expectedLeafCount)
	}
	treeRoot := build(stateHashes, 0)
	return &Tree{
		jobID:     jobID,
		leafCount: expectedLeafCount,
		root:      bindRoot(jobID, expectedLeafCount, treeRoot.hash),
		treeRoot:  treeRoot,
	}, nil
}

// Root returns the root bound to the job ID and expected leaf count.
func (t *Tree) Root() Digest {
	return t.root
}

// Proof returns the inclusion path for index, ordered from leaf to root.
func (t *Tree) Proof(index uint64) (Proof, error) {
	if index >= t.leafCount {
		return Proof{}, fmt.Errorf("%w: index %d, leaf count %d", ErrIndexOutOfRange, index, t.leafCount)
	}
	siblings := make([]Digest, 0, pathLength(index, t.leafCount))
	collectProof(t.treeRoot, index, &siblings)
	return Proof{Index: index, Siblings: siblings}, nil
}

// Verify checks a state-hash inclusion proof against trusted job context and
// an advertised bound root.
func Verify(root, jobID Digest, leafCount uint64, stateHash Digest, proof Proof) bool {
	if leafCount == 0 || proof.Index >= leafCount {
		return false
	}
	if len(proof.Siblings) != pathLength(proof.Index, leafCount) {
		return false
	}
	position := 0
	treeRoot := rebuildRoot(
		leafHash(proof.Index, stateHash),
		proof.Index,
		leafCount,
		proof.Siblings,
		&position,
	)
	return position == len(proof.Siblings) && bindRoot(jobID, leafCount, treeRoot) == root
}

func build(stateHashes []Digest, offset uint64) *node {
	if len(stateHashes) == 1 {
		return &node{hash: leafHash(offset, stateHashes[0]), leafCount: 1}
	}
	split := splitPoint(uint64(len(stateHashes)))
	left := build(stateHashes[:int(split)], offset)
	right := build(stateHashes[int(split):], offset+split)
	return &node{
		hash:      innerHash(left.hash, right.hash),
		leafCount: uint64(len(stateHashes)),
		left:      left,
		right:     right,
	}
}

func collectProof(current *node, index uint64, siblings *[]Digest) {
	if current.leafCount == 1 {
		return
	}
	if index < current.left.leafCount {
		collectProof(current.left, index, siblings)
		*siblings = append(*siblings, current.right.hash)
		return
	}
	collectProof(current.right, index-current.left.leafCount, siblings)
	*siblings = append(*siblings, current.left.hash)
}

func rebuildRoot(current Digest, index, leafCount uint64, siblings []Digest, position *int) Digest {
	if leafCount == 1 {
		return current
	}
	split := splitPoint(leafCount)
	if index < split {
		left := rebuildRoot(current, index, split, siblings, position)
		right := siblings[*position]
		*position = *position + 1
		return innerHash(left, right)
	}
	right := rebuildRoot(current, index-split, leafCount-split, siblings, position)
	left := siblings[*position]
	*position = *position + 1
	return innerHash(left, right)
}

func pathLength(index, leafCount uint64) int {
	if leafCount == 1 {
		return 0
	}
	split := splitPoint(leafCount)
	if index < split {
		return pathLength(index, split) + 1
	}
	return pathLength(index-split, leafCount-split) + 1
}

// splitPoint returns the largest power of two strictly below leafCount.
func splitPoint(leafCount uint64) uint64 {
	return uint64(1) << (bits.Len64(leafCount-1) - 1)
}

func leafHash(index uint64, stateHash Digest) Digest {
	var input [1 + 8 + sha256.Size]byte
	input[0] = 0
	binary.LittleEndian.PutUint64(input[1:9], index)
	copy(input[9:], stateHash[:])
	return sha256.Sum256(input[:])
}

func innerHash(left, right Digest) Digest {
	var input [1 + 2*sha256.Size]byte
	input[0] = 1
	copy(input[1:1+sha256.Size], left[:])
	copy(input[1+sha256.Size:], right[:])
	return sha256.Sum256(input[:])
}

func bindRoot(jobID Digest, leafCount uint64, treeRoot Digest) Digest {
	var input [len(rootDomain) + sha256.Size + 8 + sha256.Size]byte
	copy(input[:], rootDomain)
	offset := len(rootDomain)
	copy(input[offset:offset+sha256.Size], jobID[:])
	offset += sha256.Size
	binary.LittleEndian.PutUint64(input[offset:offset+8], leafCount)
	offset += 8
	copy(input[offset:], treeRoot[:])
	return sha256.Sum256(input[:])
}
