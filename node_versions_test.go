package iavl

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestNodeVersions_Inc(t *testing.T) {
	nv := NewNodeVersions(10, 20, 0)
	nv.Inc(1, 10)
	require.Equal(t, 1, len(nv.changes))
	require.Equal(t, 10, nv.changes[1])
	nv.Inc1(1)
	require.Equal(t, 1, len(nv.changes))
	require.Equal(t, 11, nv.changes[1])
	nv.Dec(10, 5)
	require.Equal(t, 2, len(nv.changes))
	require.Equal(t, -5, nv.changes[10])
	require.Equal(t, 11, nv.changes[1])
}
