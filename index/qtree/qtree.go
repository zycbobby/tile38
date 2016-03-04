package qtree

// Item is a qtree item
type Item interface {
	Point() (x, y float64)
}

// Point is point
type Point struct {
	X, Y float64
}

// Point returns the point
func (item *Point) Point() (x, y float64) {
	return item.X, item.Y
}

const maxPoints = 16
const appendGrowth = false // Set 'true' for faster inserts, Set 'false' for smaller memory size

type nodeT struct {
	count  int
	points []Item
	nodes  [4]*nodeT
}

// QTree is an implentation of a quad tree
type QTree struct {
	root       *nodeT
	minX, minY float64
	maxX, maxY float64
}

// New creates a new QTree
func New(minX, minY, maxX, maxY float64) *QTree {
	return &QTree{&nodeT{}, minX, minY, maxX, maxY}
}

func (tr *QTree) clip(item Item) (float64, float64) {
	x, y := item.Point()
	if x < tr.minX {
		x = tr.minX
	} else if x > tr.maxX {
		x = tr.maxX
	}
	if y < tr.minY {
		y = tr.minY
	} else if y > tr.maxY {
		y = tr.maxY
	}
	return x, y
}

func split(minX, minY, maxX, maxY, cx, cy float64) (quad int, nMinX, nMinY, nMaxX, nMaxY float64) {
	if cx < (maxX-minX)/2+minX {
		if cy < (maxY-minY)/2+minY {
			return 2, minX, minY, (maxX-minX)/2 + minX, (maxY-minY)/2 + minY
		}
		return 0, minX, (maxY-minY)/2 + minY, (maxX-minX)/2 + minX, maxY
	}
	if cy < (maxY-minY)/2+minY {
		return 3, (maxX-minX)/2 + minX, minY, maxX, (maxY-minY)/2 + minY
	}
	return 1, (maxX-minX)/2 + minX, (maxY-minY)/2 + minY, maxX, maxY
}

// Insert inserts an item into the tree
func (tr *QTree) Insert(item Item) {
	cx, cy := tr.clip(item)
	insert(tr.root, tr.minX, tr.minY, tr.maxX, tr.maxY, cx, cy, item)
}

// Remove removes an item from the tree
func (tr *QTree) Remove(item Item) {
	cx, cy := tr.clip(item)
	remove(tr.root, tr.minX, tr.minY, tr.maxX, tr.maxY, cx, cy, item)
}

// Search finds all items contained in a bounding box
func (tr *QTree) Search(minX, minY, maxX, maxY float64, iterator func(item Item) bool) {
	search(tr.root, tr.minX, tr.minY, tr.maxX, tr.maxY, minX, minY, maxX, maxY, true, iterator)
}

// Count counts all of the items in the tree
func (tr *QTree) Count() int {
	return count(tr.root, 0)
}

// RemoveAll removes all items from the tree
func (tr *QTree) RemoveAll() {
	tr.root = &nodeT{}
}

func insert(node *nodeT, nMinX, nMinY, nMaxX, nMaxY, cx, cy float64, item Item) {
	if node.count < maxPoints {
		if len(node.points) == node.count {
			if appendGrowth {
				node.points = append(node.points, item)
			} else {
				npoints := make([]Item, node.count+1)
				copy(npoints, node.points)
				node.points = npoints
				node.points[node.count] = item
			}
		} else {
			node.points[node.count] = item
		}
		node.count++
	} else {
		var quad int
		quad, nMinX, nMinY, nMaxX, nMaxY = split(nMinX, nMinY, nMaxX, nMaxY, cx, cy)
		if node.nodes[quad] == nil {
			node.nodes[quad] = &nodeT{}
		}
		insert(node.nodes[quad], nMinX, nMinY, nMaxX, nMaxY, cx, cy, item)
	}
}

func remove(node *nodeT, nMinX, nMinY, nMaxX, nMaxY, cx, cy float64, item Item) {
	for i := 0; i < node.count; i++ {
		if node.points[i] == item {
			node.points[i] = node.points[node.count-1]
			node.count--
			return
		}
	}
	var quad int
	quad, nMinX, nMinY, nMaxX, nMaxY = split(nMinX, nMinY, nMaxX, nMaxY, cx, cy)
	if node.nodes[quad] != nil {
		remove(node.nodes[quad], nMinX, nMinY, nMaxX, nMaxY, cx, cy, item)
	}
}

func count(node *nodeT, counter int) int {
	counter += node.count
	for i := 0; i < 4; i++ {
		if node.nodes[i] != nil {
			counter = count(node.nodes[i], counter)
		}
	}
	return counter
}

func doesOverlap(nMinX, nMinY, nMaxX, nMaxY float64, minX, minY, maxX, maxY float64) bool {
	if nMinX > maxX || minX > nMaxX {
		return false
	}
	if nMinY > maxY || minY > nMaxY {
		return false
	}
	return true
}

func search(node *nodeT, nMinX, nMinY, nMaxX, nMaxY float64, minX, minY, maxX, maxY float64, overlap bool, iterator func(item Item) bool) bool {
	if overlap {
		overlap = doesOverlap(nMinX, nMinY, nMaxX, nMaxY, minX, minY, maxX, maxY)
	}
	if !overlap {
		return true
	}
	for i := 0; i < node.count; i++ {
		item := node.points[i]
		x, y := item.Point()
		if x >= minX && x <= maxX && y >= minY && y <= maxY {
			if !iterator(item) {
				return false
			}
		}
	}
	var qMinX, qMaxX, qMinY, qMaxY float64
	for i := 0; i < 4; i++ {
		if node.nodes[i] != nil {
			switch i {
			case 0:
				qMinX, qMinY, qMaxX, qMaxY = nMinX, (nMaxY-nMinY)/2+nMinY, (nMaxX-nMinX)/2+nMinX, nMaxY
			case 1:
				qMinX, qMinY, qMaxX, qMaxY = (nMaxX-nMinX)/2+nMinX, (nMaxY-nMinY)/2+nMinY, nMaxX, nMaxY
			case 2:
				qMinX, qMinY, qMaxX, qMaxY = nMinX, nMinY, (nMaxX-nMinX)/2+nMinX, (nMaxY-nMinY)/2+nMinY
			case 3:
				qMinX, qMinY, qMaxX, qMaxY = (nMaxX-nMinX)/2+nMinX, nMinY, nMaxX, (nMaxY-nMinY)/2+nMinY
			}
			if !search(node.nodes[i], qMinX, qMinY, qMaxX, qMaxY, minX, minY, maxX, maxY, overlap, iterator) {
				return false
			}
		}
	}
	return true
}
