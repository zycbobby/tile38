package qtree

import (
	"math/rand"
	"runtime"
	"testing"
)

func randf(min, max float64) float64 {
	return rand.Float64()*(max-min) + min
}
func randXY() (x float64, y float64) {
	return randf(0, 100), randf(0, 100)
}
func randPoint() (lat float64, lon float64) {
	return randf(-90, 90), randf(-180, 180)
}

func wp(x, y float64) *Point {
	return &Point{x, y}
}

func TestClip(t *testing.T) {
	tr := New(-180, -90, 180, 90)
	if x, y := tr.clip(wp(-900, 100)); x != -180 || y != 90 {
		t.Fatalf("x,y == %f,%f, expect %f,%f", x, y, -180.0, 90.0)
	}
	if x, y := tr.clip(wp(900, -100)); x != 180 || y != -90 {
		t.Fatalf("x,y == %f,%f, expect %f,%f", x, y, 180.0, -90.0)
	}
	if x, y := tr.clip(wp(100, 100)); x != 100 || y != 90 {
		t.Fatalf("x,y == %f,%f, expect %f,%f", x, y, 100.0, 90.0)
	}
	if x, y := tr.clip(wp(50, 50)); x != 50 || y != 50 {
		t.Fatalf("x,y == %f,%f, expect %f,%f", x, y, 50.0, 50.0)
	}
	if x, y := tr.clip(wp(-50, -50)); x != -50 || y != -50 {
		t.Fatalf("x,y == %f,%f, expect %f,%f", x, y, -50.0, -50.0)
	}
}

func TestSimpleSplit(t *testing.T) {
	quad, nMinX, nMinY, nMaxX, nMaxY := split(0, 0, 100, 100, 0, 100)
	if quad != 0 || nMinX != 0 || nMinY != 50 || nMaxX != 50 || nMaxY != 100 {
		t.Fatalf("failed 0: %d, %f, %f, %f, %f\n", quad, nMinX, nMinY, nMaxX, nMaxY)
	}
	quad, nMinX, nMinY, nMaxX, nMaxY = split(0, 0, 100, 100, 100, 100)
	if quad != 1 || nMinX != 50 || nMinY != 50 || nMaxX != 100 || nMaxY != 100 {
		t.Fatalf("failed 1: %d, %f, %f, %f, %f\n", quad, nMinX, nMinY, nMaxX, nMaxY)
	}
	quad, nMinX, nMinY, nMaxX, nMaxY = split(0, 0, 100, 100, 0, 0)
	if quad != 2 || nMinX != 0 || nMinY != 0 || nMaxX != 50 || nMaxY != 50 {
		t.Fatalf("failed 2: %d, %f, %f, %f, %f\n", quad, nMinX, nMinY, nMaxX, nMaxY)
	}
	quad, nMinX, nMinY, nMaxX, nMaxY = split(0, 0, 100, 100, 100, 0)
	if quad != 3 || nMinX != 50 || nMinY != 0 || nMaxX != 100 || nMaxY != 50 {
		t.Fatalf("failed 3: %d, %f, %f, %f, %f\n", quad, nMinX, nMinY, nMaxX, nMaxY)
	}
}

func TestGeoSplit(t *testing.T) {
	quad, nMinX, nMinY, nMaxX, nMaxY := split(-180, -90, 180, 90, -180, 90)
	if quad != 0 || nMinX != -180 || nMinY != 0 || nMaxX != 0 || nMaxY != 90 {
		t.Fatalf("failed 0: %d, %f, %f, %f, %f\n", quad, nMinX, nMinY, nMaxX, nMaxY)
	}
	quad, nMinX, nMinY, nMaxX, nMaxY = split(-180, -90, 180, 90, 180, 90)
	if quad != 1 || nMinX != 0 || nMinY != 0 || nMaxX != 180 || nMaxY != 90 {
		t.Fatalf("failed 1: %d, %f, %f, %f, %f\n", quad, nMinX, nMinY, nMaxX, nMaxY)
	}
	quad, nMinX, nMinY, nMaxX, nMaxY = split(-180, -90, 180, 90, -180, -90)
	if quad != 2 || nMinX != -180 || nMinY != -90 || nMaxX != 0 || nMaxY != 0 {
		t.Fatalf("failed 2: %d, %f, %f, %f, %f\n", quad, nMinX, nMinY, nMaxX, nMaxY)
	}
	quad, nMinX, nMinY, nMaxX, nMaxY = split(-180, -90, 180, 90, 180, -90)
	if quad != 3 || nMinX != 0 || nMinY != -90 || nMaxX != 180 || nMaxY != 0 {
		t.Fatalf("failed 3: %d, %f, %f, %f, %f\n", quad, nMinX, nMinY, nMaxX, nMaxY)
	}
}

func TestGeoInsert(t *testing.T) {
	tr := New(-180, -90, 180, 90)
	l := 50000
	for i := 0; i < l; i++ {
		swLat, swLon := randPoint()
		tr.Insert(wp(swLon, swLat))
	}
	count := 0
	tr.Search(-180, -90, 180, 90, func(item Item) bool {
		count++
		return true
	})
	if count != l {
		t.Fatalf("count == %d, expect %d", count, l)
	}
}

func TestMemory(t *testing.T) {
	rand.Seed(0)
	tr := New(0, 0, 100, 100)
	for i := 0; i < 500000; i++ {
		x, y := randXY()
		tr.Insert(wp(x, y))
	}
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	println(int(m.HeapAlloc)/tr.Count(), "bytes/point")
}

func BenchmarkInsert(b *testing.B) {
	rand.Seed(0)
	tr := New(0, 0, 100, 100)
	for i := 0; i < b.N; i++ {
		x, y := randXY()
		tr.Insert(wp(x, y))
	}
	count := 0
	tr.Search(0, 0, 100, 100, func(item Item) bool {
		count++
		return true
	})
	if count != b.N {
		b.Fatalf("count == %d, expect %d", count, b.N)
	}

	// tr.Search(0, 0, 100, 100, func(id int) bool {
	// 	count++
	// 	return true
	// })
	//println(tr.Count())
}
