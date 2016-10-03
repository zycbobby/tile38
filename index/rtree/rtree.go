package rtree

// Item is an rtree item
type Item interface {
	Rect() (minX, minY, maxX, maxY float64)
}

// Rect is a rectangle
type Rect struct {
	MinX, MinY, MaxX, MaxY float64
}

// Rect returns the rectangle
func (item *Rect) Rect() (minX, minY, maxX, maxY float64) {
	return item.MinX, item.MinY, item.MaxX, item.MaxY
}

// RTree is an implementation of an rtree
type RTree struct {
	tr *d2RTree
}

// New creates a new RTree
func New() *RTree {
	return &RTree{
		tr: d2New(),
	}
}

// Insert inserts item into rtree
func (tr *RTree) Insert(item Item) {
	minX, minY, maxX, maxY := item.Rect()
	tr.tr.Insert([2]float64{minX, minY}, [2]float64{maxX, maxY}, item)
}

// Remove removes item from rtree
func (tr *RTree) Remove(item Item) {
	minX, minY, maxX, maxY := item.Rect()
	tr.tr.Remove([2]float64{minX, minY}, [2]float64{maxX, maxY}, item)
}

// Search finds all items in bounding box.
func (tr *RTree) Search(minX, minY, maxX, maxY float64, iterator func(item Item) bool) {
	tr.tr.Search([2]float64{minX, minY}, [2]float64{maxX, maxY}, func(data interface{}) bool {
		return iterator(data.(Item))
	})
}

// Count return the number of items in rtree.
func (tr *RTree) Count() int {
	return tr.tr.Count()
}

// RemoveAll removes all items from rtree.
func (tr *RTree) RemoveAll() {
	tr.tr.RemoveAll()
}

func (tr *RTree) Bounds() (minX, minY, maxX, maxY float64) {
	var rect d2rectT
	if tr.tr.root != nil {
		if tr.tr.root.count > 0 {
			rect = tr.tr.root.branch[0].rect
			for i := 1; i < tr.tr.root.count; i++ {
				rect2 := tr.tr.root.branch[i].rect
				rect = d2combineRect(&rect, &rect2)
			}
		}
	}
	minX, minY, maxX, maxY = rect.min[0], rect.min[1], rect.max[0], rect.max[1]
	return
}
