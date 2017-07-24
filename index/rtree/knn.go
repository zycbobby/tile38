package rtree

import (
	"github.com/tidwall/tinyqueue"
)

type queueItem struct {
	node   *d3nodeT
	data   interface{}
	isItem bool
	dist   float64
}

func (item *queueItem) Less(b tinyqueue.Item) bool {
	return item.dist < b.(*queueItem).dist
}
func boxDistPoint(point []float64, childBox d3rectT) float64 {
	var dist float64
	for i := 0; i < len(point); i++ {
		d := axisDist(point[i], float64(childBox.min[i]), float64(childBox.max[i]))
		dist += d * d
	}
	return dist
}
func axisDist(k, min, max float64) float64 {
	if k < min {
		return min - k
	}
	if k <= max {
		return 0
	}
	return k - max
}

// NearestNeighbors gets the closest Spatials to the Point.
func (tr *RTree) NearestNeighbors(x, y, z float64, iter func(item Item, dist float64) bool) bool {
	knnPoint := []float64{x, y, z}
	queue := tinyqueue.New(nil)
	node := tr.tr.root
	for node != nil {
		for i := 0; i < node.count; i++ {
			child := node.branch[i]
			dist := boxDistPoint(knnPoint, node.branch[i].rect)
			queue.Push(&queueItem{node: child.child, data: child.data, isItem: node.isLeaf(), dist: dist})
		}
		for queue.Len() > 0 && queue.Peek().(*queueItem).isItem {
			item := queue.Pop().(*queueItem)
			if !iter(item.data.(Item), item.dist) {
				return false
			}
		}
		last := queue.Pop()
		if last != nil {
			node = last.(*queueItem).node
		} else {
			node = nil
		}
	}
	return true
}
