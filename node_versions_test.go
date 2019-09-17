package iavl

import "testing"

func BenchmarkInt64Ring_Get(b *testing.B) {
	//nums := make([]int64, 1000000, 1000000)
	b.ResetTimer()
	for i:=0;i<b.N;i++ {
		for i:=0; i<1000000;i++ {

		}
	}
}
