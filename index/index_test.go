package index

import (
	"fmt"
	"math/rand"
	"runtime"
	"testing"
	"time"
)

func randf(min, max float64) float64 {
	return rand.Float64()*(max-min) + min
}

func randPoint() (lat float64, lon float64) {
	return randf(-90, 90), randf(-180, 180)
}

func randRect() (swLat, swLon, neLat, neLon float64) {
	swLat, swLon = randPoint()
	neLat = randf(swLat-10, swLat+10)
	neLon = randf(swLon-10, swLon+10)
	return
}

func wp(swLat, swLon, neLat, neLon float64) *FlexItem {
	return &FlexItem{
		MinX: swLon,
		MinY: swLat,
		MaxX: neLon,
		MaxY: neLat,
	}
}

func TestRandomInserts(t *testing.T) {
	rand.Seed(0) //time.Now().UnixNano())
	l := 1000000
	tr := New()
	start := time.Now()
	i := 0
	for ; i < l/2; i++ {
		swLat, swLon := randPoint()
		tr.Insert(wp(swLat, swLon, swLat, swLon))
	}
	inspdur := time.Now().Sub(start)

	start = time.Now()
	for ; i < l; i++ {
		swLat, swLon, neLat, neLon := randRect()
		tr.Insert(wp(swLat, swLon, neLat, neLon))
	}
	insrdur := time.Now().Sub(start)

	count := tr.Count()
	if count != l {
		t.Fatalf("count == %d, expect %d", count, l)
	}
	count = 0
	tr.Search(0, -90, -180, 90, 180, func(item Item) bool {
		count++
		return true
	})
	if count != l {
		t.Fatalf("count == %d, expect %d", count, l)
	}
	start = time.Now()
	count = 0
	tr.Search(0, 33, -115, 34, -114, func(item Item) bool {
		count++
		return true
	})
	searchdur := time.Now().Sub(start)

	fmt.Printf("Randomly inserted %d points in %s.\n", l/2, inspdur.String())
	fmt.Printf("Randomly inserted %d rects in %s.\n", l/2, insrdur.String())
	fmt.Printf("Searched %d items in %s.\n", count, searchdur.String())
}

func TestMemory(t *testing.T) {
	rand.Seed(0)
	l := 100000
	tr := New()
	for i := 0; i < l; i++ {
		swLat, swLon, neLat, neLon := randRect()
		if rand.Int()%2 == 0 { // one in three chance that the rect is actually a point.
			neLat, neLon = swLat, swLon
		}
		tr.Insert(wp(swLat, swLon, neLat, neLon))
	}
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	const PtrSize = 32 << uintptr(uint64(^uintptr(0))>>63)
	fmt.Printf("Memory consumption is %d bytes/object. Pointers are %d bytes.\n", int(m.HeapAlloc)/tr.Count(), PtrSize/8)
}

func TestInsertVarious(t *testing.T) {
	var count int
	tr := New()
	item := wp(33, -115, 33, -115)
	tr.Insert(item)
	count = tr.Count()
	if count != 1 {
		t.Fatalf("count = %d, expect 1", count)
	}
	tr.Remove(item)
	count = tr.Count()
	if count != 0 {
		t.Fatalf("count = %d, expect 0", count)
	}
	tr.Insert(item)
	count = tr.Count()
	if count != 1 {
		t.Fatalf("count = %d, expect 1", count)
	}
	found := false
	tr.Search(0, -90, -180, 90, 180, func(item2 Item) bool {
		if item2 == item {
			found = true
		}
		return true
	})
	if !found {
		t.Fatal("did not find item")
	}
}

func BenchmarkInsertRect(b *testing.B) {
	rand.Seed(time.Now().UnixNano())
	tr := New()
	for i := 0; i < b.N; i++ {
		swLat, swLon, neLat, neLon := randRect()
		tr.Insert(wp(swLat, swLon, neLat, neLon))
	}
}

func BenchmarkInsertPoint(b *testing.B) {
	rand.Seed(time.Now().UnixNano())
	tr := New()
	for i := 0; i < b.N; i++ {
		swLat, swLon, _, _ := randRect()
		tr.Insert(wp(swLat, swLon, swLat, swLon))
	}
}

func BenchmarkInsertEither(b *testing.B) {
	rand.Seed(time.Now().UnixNano())
	tr := New()
	for i := 0; i < b.N; i++ {
		swLat, swLon, neLat, neLon := randRect()
		if rand.Int()%3 == 0 { // one in three chance that the rect is actually a point.
			neLat, neLon = swLat, swLon
		}
		tr.Insert(wp(swLat, swLon, neLat, neLon))
	}
}

// func BenchmarkSearchRect(b *testing.B) {
// 	rand.Seed(time.Now().UnixNano())
// 	tr := New()
// 	for i := 0; i < 100000; i++ {
// 		swLat, swLon, neLat, neLon := randRect()
// 		tr.Insert(swLat, swLon, neLat, neLon)
// 	}
// 	b.ResetTimer()
// 	count := 0
// 	//for i := 0; i < b.N; i++ {
// 	tr.Search(0, -180, 90, 180, func(id int) bool {
// 		count++
// 		return true
// 	})
// 	//}
// 	println(count)
// }
