package index

import (
	"math"
	"unsafe"

	"github.com/tidwall/tile38/index/rtree"
)

// Item represents an index item.
type Item interface {
	Point() (x, y, z float64)
	Rect() (minX, minY, minZ, maxX, maxY, maxZ float64)
}

// FlexItem can represent a point or a rectangle
type FlexItem struct {
	MinX, MinY, MinZ, MaxX, MaxY, MaxZ float64
}

// Rect returns the rectangle
func (item *FlexItem) Rect() (minX, minY, minZ, maxX, maxY, maxZ float64) {
	return item.MinX, item.MinY, item.MinZ, item.MaxX, item.MaxY, item.MaxZ
}

// Point returns the point
func (item *FlexItem) Point() (x, y, z float64) {
	return item.MinX, item.MinY, item.MinZ
}

// Index is a geospatial index
type Index struct {
	r    *rtree.RTree
	nr   map[*rtree.Rect]Item   // normalized points
	nrr  map[Item][]*rtree.Rect // normalized points
	mulm map[interface{}]bool   // store items that contain multiple rects
}

// New create a new index
func New() *Index {
	return &Index{
		r:    rtree.New(),
		mulm: make(map[interface{}]bool),
		nr:   make(map[*rtree.Rect]Item),
		nrr:  make(map[Item][]*rtree.Rect),
	}
}

// Insert inserts an item into the index
func (ix *Index) Insert(item Item) {
	minX, minY, minZ, maxX, maxY, maxZ := item.Rect()
	if minX == maxX && minY == maxY {
		x, y, normd := normPoint(minY, minX)
		if normd {
			nitem := &rtree.Rect{MinX: x, MinY: y, MinZ: minZ, MaxX: x, MaxY: y, MaxZ: maxZ}
			ix.nr[nitem] = item
			ix.nrr[item] = []*rtree.Rect{nitem}
			ix.r.Insert(nitem)
		} else {
			ix.r.Insert(item)
		}
	} else {
		mins, maxs, normd := normRect(minY, minX, maxY, maxX)
		if normd {
			var nitems []*rtree.Rect
			for i := range mins {
				minX, minY, maxX, maxY := mins[i][0], mins[i][1], maxs[i][0], maxs[i][1]
				nitem := &rtree.Rect{MinX: minX, MinY: minY, MinZ: minZ, MaxX: maxX, MaxY: maxY, MaxZ: maxZ}
				ix.nr[nitem] = item
				nitems = append(nitems, nitem)
				ix.r.Insert(nitem)
			}
			ix.nrr[item] = nitems
			if len(mins) > 1 {
				ix.mulm[item] = true
			}
		} else {
			ix.r.Insert(item)
		}
	}
	return
}

// Remove removed an item from the index
func (ix *Index) Remove(item Item) {
	if nitems, ok := ix.nrr[item]; ok {
		for _, nitem := range nitems {
			ix.r.Remove(nitem)
			delete(ix.nr, nitem)
		}
		delete(ix.nrr, item)
	} else {
		ix.r.Remove(item)
	}
}

// Count counts all items in the index.
func (ix *Index) Count() int {
	count := 0
	ix.Search(-90, -180, 90, 180, math.Inf(-1), math.Inf(+1), func(_ interface{}) bool {
		count++
		return true
	})
	return count
}

// Bounds returns the minimum bounding rectangle of all items in the index.
func (ix *Index) Bounds() (MinX, MinY, MaxX, MaxY float64) {
	return ix.r.Bounds()
}

// RemoveAll removes all items from the index.
func (ix *Index) RemoveAll() {
	ix.r.RemoveAll()
}

type UintptrInterface struct {
	Type uintptr
	Ptr  uintptr
}
type UnsafePointerInterface struct {
	Type uintptr
	Ptr  unsafe.Pointer
}

func GetUintptrInterface(v interface{}) UintptrInterface {
	return *(*UintptrInterface)(unsafe.Pointer(&v))
}

func GetUnsafePointerInterface(v interface{}) UnsafePointerInterface {
	return *(*UnsafePointerInterface)(unsafe.Pointer(&v))
}

var rectType = func() uintptr {
	var rrrr rtree.Rect
	return GetUintptrInterface(&rrrr).Type
}()

func (ix *Index) getRTreeItem(item interface{}) interface{} {
	uzi := GetUnsafePointerInterface(item)
	if uzi.Type == rectType {
		return ix.nr[(*rtree.Rect)(uzi.Ptr)]
	}
	return item
}

func (ix *Index) NearestNeighbors(lat, lon float64, iterator func(item interface{}) bool) bool {
	x, y, _ := normPoint(lat, lon)
	return ix.r.NearestNeighbors(x, y, func(item interface{}, dist float64) bool {
		return iterator(ix.getRTreeItem(item))
	})
}

// Search returns all items that intersect the bounding box.
func (ix *Index) Search(swLat, swLon, neLat, neLon, minZ, maxZ float64,
	iterator func(item interface{}) bool,
) bool {
	var keepon = true
	var idm = make(map[interface{}]bool)
	mins, maxs, _ := normRect(swLat, swLon, neLat, neLon)
	// Points
	if len(mins) == 1 {
		// There is only one rectangle.
		// It's possible that a r rect may span multiple entries. Check mulm map for spanning rects.
		if keepon {
			ix.r.Search(mins[0][0], mins[0][1], minZ, maxs[0][0], maxs[0][1], maxZ,
				func(v interface{}) bool {
					item := ix.getRTreeItem(v)
					if len(ix.mulm) > 0 && ix.mulm[item] {
						if !idm[item] {
							idm[item] = true
							keepon = iterator(item)
						}
					} else {
						keepon = iterator(item)
					}
					return keepon
				},
			)
		}
	} else {
		// There are multiple rectangles. Duplicates might occur.
		for i := range mins {
			if keepon {
				ix.r.Search(mins[i][0], mins[i][1], minZ, maxs[i][0], maxs[i][1], maxZ,
					func(item interface{}) bool {
						iitm := ix.getRTreeItem(item)
						if iitm != nil {
							if ix.mulm[iitm] {
								if !idm[iitm] {
									idm[iitm] = true
									keepon = iterator(iitm)
								}
							} else {
								keepon = iterator(iitm)
							}
						}
						return keepon
					},
				)
			}
		}
	}
	return keepon
}
