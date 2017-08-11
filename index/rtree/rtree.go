package rtree

import "github.com/tidwall/tile38/index/rtreebase"

// Item is an rtree item
type Item interface {
	Rect() (minX, minY, minZ, maxX, maxY, maxZ float64)
}

// Rect is a rectangle
type Rect struct {
	MinX, MinY, MinZ, MaxX, MaxY, MaxZ float64
}

// Rect returns the rectangle
func (item *Rect) Rect() (minX, minY, minZ, maxX, maxY, maxZ float64) {
	return item.MinX, item.MinY, item.MinZ, item.MaxX, item.MaxY, item.MaxZ
}

// RTree is an implementation of an rtree
type RTree struct {
	tr *rtreebase.RTree
}

// New creates a new RTree
func New() *RTree {
	return &RTree{
		tr: rtreebase.New(),
	}
}

// Insert inserts item into rtree
func (tr *RTree) Insert(item Item) {
	minX, minY, _, maxX, maxY, _ := item.Rect()
	tr.tr.Insert([2]float64{minX, minY}, [2]float64{maxX, maxY}, item)
}

// Remove removes item from rtree
func (tr *RTree) Remove(item Item) {
	minX, minY, _, maxX, maxY, _ := item.Rect()
	tr.tr.Remove([2]float64{minX, minY}, [2]float64{maxX, maxY}, item)
}

// Search finds all items in bounding box.
func (tr *RTree) Search(minX, minY, minZ, maxX, maxY, maxZ float64, iterator func(data interface{}) bool) {
	// start := time.Now()
	// var count int
	tr.tr.Search([2]float64{minX, minY}, [2]float64{maxX, maxY}, func(data interface{}) bool {
		// count++
		return iterator(data)
	})
	// dur := time.Since(start)
	// fmt.Printf("%s %d\n", dur, count)
}

// Count return the number of items in rtree.
func (tr *RTree) Count() int {
	return tr.tr.Count()
}

// RemoveAll removes all items from rtree.
func (tr *RTree) RemoveAll() {
	tr.tr = rtreebase.New()
}

// Bounds returns the bounds of the R-tree
func (tr *RTree) Bounds() (minX, minY, maxX, maxY float64) {
	min, max := tr.tr.Bounds()
	return min[0], min[1], max[0], max[1]
}

// NearestNeighbors gets the closest Spatials to the Point.
func (tr *RTree) NearestNeighbors(x, y float64, iter func(item interface{}, dist float64) bool) bool {
	return tr.tr.KNN([2]float64{x, y}, [2]float64{x, y}, true, func(item interface{}, dist float64) bool {
		return iter(item, dist)
	})
}
