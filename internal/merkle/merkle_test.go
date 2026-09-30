package merkle

import (
	"crypto/sha256"
	"encoding/binary"
	"slices"
	"testing"

	"github.com/0xciph3r/mini-verde/internal/job"
)

func TestCommitsInitialAndEveryPostStepState(t *testing.T) {
	t.Parallel()

	spec := job.JobSpec{
		Version: job.ProtocolVersion, Seed: 7, Steps: 3,
		InputSize: 1, HiddenSize: 1, OutputSize: 1,
		BatchSize: 1, LearningRate: job.DefaultLearningRate,
		Dataset: job.Dataset{Examples: 1, Inputs: []float32{0.5}, Targets: []float32{0.25}},
	}
	machine, err := job.NewMachine(spec)
	if err != nil {
		t.Fatalf("job.NewMachine() error = %v", err)
	}
	state := machine.InitialState()
	stateHashes := make([]Digest, 0, machine.LeafCount())
	for {
		stateHash, err := machine.HashState(state)
		if err != nil {
			t.Fatalf("HashState(step=%d) error = %v", state.Step, err)
		}
		stateHashes = append(stateHashes, stateHash)
		if state.Step == spec.Steps {
			break
		}
		state, err = machine.Step(state)
		if err != nil {
			t.Fatalf("Step(step=%d) error = %v", state.Step, err)
		}
	}
	if got, want := uint64(len(stateHashes)), spec.Steps+1; got != want {
		t.Fatalf("state hash count = %d, want %d", got, want)
	}
	tree, err := New(machine.ID(), machine.LeafCount(), stateHashes)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	for index, stateHash := range stateHashes {
		proof, err := tree.Proof(uint64(index))
		if err != nil {
			t.Fatalf("Proof(%d) error = %v", index, err)
		}
		if !Verify(tree.Root(), machine.ID(), machine.LeafCount(), stateHash, proof) {
			t.Fatalf("Verify(step=%d) = false", index)
		}
	}
}

func TestTreeMatchesNaiveRecursiveBuilder(t *testing.T) {
	t.Parallel()

	jobID := testDigest("differential-job")
	for leafCount := 1; leafCount <= 200; leafCount++ {
		stateHashes := testStateHashes(leafCount, "differential-leaf")
		tree, err := New(jobID, uint64(leafCount), stateHashes)
		if err != nil {
			t.Fatalf("New(n=%d) error = %v", leafCount, err)
		}
		wantTreeRoot := referenceTreeHash(stateHashes, 0)
		wantRoot := referenceBoundRoot(jobID, uint64(leafCount), wantTreeRoot)
		if got := tree.Root(); got != wantRoot {
			t.Fatalf("Root(n=%d) = %x, want %x", leafCount, got, wantRoot)
		}

		for index := range leafCount {
			proof, err := tree.Proof(uint64(index))
			if err != nil {
				t.Fatalf("Proof(n=%d, index=%d) error = %v", leafCount, index, err)
			}
			if got, want := len(proof.Siblings), referencePathLength(uint64(index), uint64(leafCount)); got != want {
				t.Fatalf("proof length (n=%d, index=%d) = %d, want %d", leafCount, index, got, want)
			}
			if !Verify(wantRoot, jobID, uint64(leafCount), stateHashes[index], proof) {
				t.Fatalf("Verify(n=%d, index=%d) = false", leafCount, index)
			}
		}
	}
}

func TestProofsAtBoundaries(t *testing.T) {
	t.Parallel()

	const leafCount = 9
	jobID := testDigest("boundary-job")
	stateHashes := testStateHashes(leafCount, "boundary-leaf")
	tree, err := New(jobID, leafCount, stateHashes)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	for _, index := range []uint64{0, 7, 8} {
		proof, err := tree.Proof(index)
		if err != nil {
			t.Fatalf("Proof(%d) error = %v", index, err)
		}
		if !Verify(tree.Root(), jobID, leafCount, stateHashes[index], proof) {
			t.Errorf("Verify(index=%d) = false", index)
		}
	}
}

func TestProofRejectsWrongSiblingCountAndOutOfRangeIndex(t *testing.T) {
	t.Parallel()

	const leafCount = 7
	jobID := testDigest("malformed-proof-job")
	stateHashes := testStateHashes(leafCount, "malformed-proof-leaf")
	tree, err := New(jobID, leafCount, stateHashes)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	proof, err := tree.Proof(3)
	if err != nil {
		t.Fatalf("Proof() error = %v", err)
	}

	tooShort := cloneProof(proof)
	tooShort.Siblings = tooShort.Siblings[:len(tooShort.Siblings)-1]
	if Verify(tree.Root(), jobID, leafCount, stateHashes[3], tooShort) {
		t.Fatal("Verify() accepted a short proof")
	}
	tooLong := cloneProof(proof)
	tooLong.Siblings = append(tooLong.Siblings, testDigest("extra"))
	if Verify(tree.Root(), jobID, leafCount, stateHashes[3], tooLong) {
		t.Fatal("Verify() accepted a long proof")
	}
	outOfRange := cloneProof(proof)
	outOfRange.Index = leafCount
	if Verify(tree.Root(), jobID, leafCount, stateHashes[3], outOfRange) {
		t.Fatal("Verify() accepted index == leaf count")
	}
	if _, err := tree.Proof(leafCount); err == nil {
		t.Fatal("Proof() accepted index == leaf count")
	}
}

func TestIndexBoundLeafRejectsReindexingDuplicateStateHash(t *testing.T) {
	t.Parallel()

	jobID := testDigest("duplicate-job")
	repeated := testDigest("repeated-state-hash")
	tree, err := New(jobID, 2, []Digest{repeated, repeated})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	proof, err := tree.Proof(0)
	if err != nil {
		t.Fatalf("Proof() error = %v", err)
	}
	proof.Index = 1
	if Verify(tree.Root(), jobID, 2, repeated, proof) {
		t.Fatal("Verify() accepted a proof moved between duplicate leaves")
	}
}

func TestInnerNodeCannotOpenAsLeaf(t *testing.T) {
	t.Parallel()

	jobID := testDigest("domain-job")
	stateHashes := testStateHashes(2, "domain-leaf")
	tree, err := New(jobID, 2, stateHashes)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	proof, err := tree.Proof(0)
	if err != nil {
		t.Fatalf("Proof() error = %v", err)
	}
	inner := referenceInner(
		referenceLeaf(0, stateHashes[0]),
		referenceLeaf(1, stateHashes[1]),
	)
	if Verify(tree.Root(), jobID, 2, inner, proof) {
		t.Fatal("Verify() accepted an inner-node hash as leaf data")
	}
}

func TestDifferentListsAndLengthsHaveDifferentRoots(t *testing.T) {
	t.Parallel()

	jobID := testDigest("list-job")
	a := testDigest("a")
	b := testDigest("b")
	c := testDigest("c")
	d := testDigest("d")

	three, err := New(jobID, 3, []Digest{a, b, c})
	if err != nil {
		t.Fatalf("New(three) error = %v", err)
	}
	fourWithDuplicate, err := New(jobID, 4, []Digest{a, b, c, c})
	if err != nil {
		t.Fatalf("New(four duplicate) error = %v", err)
	}
	fourDifferent, err := New(jobID, 4, []Digest{a, b, c, d})
	if err != nil {
		t.Fatalf("New(four different) error = %v", err)
	}
	if three.Root() == fourWithDuplicate.Root() {
		t.Fatal("three leaves and duplicated-last four leaves have the same root")
	}
	if fourWithDuplicate.Root() == fourDifferent.Root() {
		t.Fatal("different four-leaf lists have the same root")
	}
}

func TestRootBindsJobAndLeafCount(t *testing.T) {
	t.Parallel()

	jobID := testDigest("bound-job")
	stateHashes := testStateHashes(3, "bound-leaf")
	tree, err := New(jobID, 3, stateHashes)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	proof, err := tree.Proof(1)
	if err != nil {
		t.Fatalf("Proof() error = %v", err)
	}
	otherJobID := jobID
	otherJobID[0] ^= 1
	if Verify(tree.Root(), otherJobID, 3, stateHashes[1], proof) {
		t.Fatal("Verify() accepted another job ID")
	}
	if Verify(tree.Root(), jobID, 4, stateHashes[1], proof) {
		t.Fatal("Verify() accepted another leaf count")
	}
}

func TestNewRejectsEmptyOrMismatchedLeafCount(t *testing.T) {
	t.Parallel()

	jobID := testDigest("invalid-tree-job")
	if _, err := New(jobID, 0, nil); err == nil {
		t.Fatal("New() accepted an empty tree")
	}
	if _, err := New(jobID, 3, testStateHashes(2, "short")); err == nil {
		t.Fatal("New() accepted fewer leaves than expected")
	}
	if _, err := New(jobID, 2, testStateHashes(3, "long")); err == nil {
		t.Fatal("New() accepted more leaves than expected")
	}
}

func FuzzProofRejectsMutation(f *testing.F) {
	f.Add([]byte("mini-verde"), uint8(7), uint8(3), uint8(0))
	f.Add([]byte{0, 1, 2, 3}, uint8(2), uint8(1), uint8(31))
	f.Fuzz(func(t *testing.T, seed []byte, countSeed, indexSeed, byteSeed uint8) {
		leafCount := 2 + int(countSeed%31)
		stateHashes := fuzzStateHashes(leafCount, seed)
		jobIDMaterial := append([]byte("fuzz-job:"), seed...)
		jobID := sha256.Sum256(jobIDMaterial)
		tree, err := New(jobID, uint64(leafCount), stateHashes)
		if err != nil {
			t.Fatalf("New() error = %v", err)
		}
		index := uint64(indexSeed) % uint64(leafCount)
		proof, err := tree.Proof(index)
		if err != nil {
			t.Fatalf("Proof() error = %v", err)
		}
		if !Verify(tree.Root(), jobID, uint64(leafCount), stateHashes[index], proof) {
			t.Fatal("baseline proof did not verify")
		}

		mutatedLeaf := stateHashes[index]
		mutatedLeaf[int(byteSeed)%len(mutatedLeaf)] ^= 1
		if Verify(tree.Root(), jobID, uint64(leafCount), mutatedLeaf, proof) {
			t.Fatal("proof verified after state-hash mutation")
		}

		for siblingIndex := range proof.Siblings {
			mutatedProof := cloneProof(proof)
			mutatedProof.Siblings[siblingIndex][int(byteSeed)%sha256.Size] ^= 1
			if Verify(tree.Root(), jobID, uint64(leafCount), stateHashes[index], mutatedProof) {
				t.Fatalf("proof verified after sibling %d mutation", siblingIndex)
			}
		}

		mutatedProof := cloneProof(proof)
		mutatedProof.Index = (proof.Index + 1) % uint64(leafCount)
		if Verify(tree.Root(), jobID, uint64(leafCount), stateHashes[index], mutatedProof) {
			t.Fatal("proof verified after index mutation")
		}
		if Verify(tree.Root(), jobID, uint64(leafCount+1), stateHashes[index], proof) {
			t.Fatal("proof verified after leaf-count mutation")
		}
		mutatedJobID := jobID
		mutatedJobID[int(byteSeed)%len(mutatedJobID)] ^= 1
		if Verify(tree.Root(), mutatedJobID, uint64(leafCount), stateHashes[index], proof) {
			t.Fatal("proof verified after job-ID mutation")
		}
	})
}

func referenceTreeHash(stateHashes []Digest, offset uint64) Digest {
	if len(stateHashes) == 1 {
		return referenceLeaf(offset, stateHashes[0])
	}
	split := referenceSplit(len(stateHashes))
	left := referenceTreeHash(stateHashes[:split], offset)
	right := referenceTreeHash(stateHashes[split:], offset+uint64(split))
	return referenceInner(left, right)
}

func referenceLeaf(index uint64, stateHash Digest) Digest {
	var input [1 + 8 + sha256.Size]byte
	input[0] = 0
	binary.LittleEndian.PutUint64(input[1:9], index)
	copy(input[9:], stateHash[:])
	return sha256.Sum256(input[:])
}

func referenceInner(left, right Digest) Digest {
	var input [1 + 2*sha256.Size]byte
	input[0] = 1
	copy(input[1:1+sha256.Size], left[:])
	copy(input[1+sha256.Size:], right[:])
	return sha256.Sum256(input[:])
}

func referenceBoundRoot(jobID Digest, leafCount uint64, treeRoot Digest) Digest {
	const domain = "mini-verde/root/v1"
	var input [len(domain) + sha256.Size + 8 + sha256.Size]byte
	copy(input[:], domain)
	offset := len(domain)
	copy(input[offset:offset+sha256.Size], jobID[:])
	offset += sha256.Size
	binary.LittleEndian.PutUint64(input[offset:offset+8], leafCount)
	offset += 8
	copy(input[offset:], treeRoot[:])
	return sha256.Sum256(input[:])
}

func referenceSplit(length int) int {
	split := 1
	for split<<1 < length {
		split <<= 1
	}
	return split
}

func referencePathLength(index, leafCount uint64) int {
	if leafCount == 1 {
		return 0
	}
	split := uint64(referenceSplit(int(leafCount)))
	if index < split {
		return referencePathLength(index, split) + 1
	}
	return referencePathLength(index-split, leafCount-split) + 1
}

func testStateHashes(count int, domain string) []Digest {
	hashes := make([]Digest, count)
	for i := range hashes {
		var input [8]byte
		binary.LittleEndian.PutUint64(input[:], uint64(i))
		hasher := sha256.New()
		_, _ = hasher.Write([]byte(domain))
		_, _ = hasher.Write(input[:])
		copy(hashes[i][:], hasher.Sum(nil))
	}
	return hashes
}

func fuzzStateHashes(count int, seed []byte) []Digest {
	hashes := make([]Digest, count)
	for i := range hashes {
		var index [8]byte
		binary.LittleEndian.PutUint64(index[:], uint64(i))
		hasher := sha256.New()
		_, _ = hasher.Write(seed)
		_, _ = hasher.Write(index[:])
		copy(hashes[i][:], hasher.Sum(nil))
	}
	return hashes
}

func testDigest(value string) Digest {
	return sha256.Sum256([]byte(value))
}

func cloneProof(proof Proof) Proof {
	return Proof{Index: proof.Index, Siblings: slices.Clone(proof.Siblings)}
}
