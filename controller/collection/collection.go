package collection

import (
	"math"

	"github.com/google/btree"
	"github.com/tidwall/tile38/geojson"
	"github.com/tidwall/tile38/index"
)

type itemT struct {
	ID     string
	Object geojson.Object
	Fields []float64
}

func (i *itemT) Less(item btree.Item) bool {
	return i.ID < item.(*itemT).ID
}

func (i *itemT) Rect() (minX, minY, maxX, maxY float64) {
	bbox := i.Object.CalculatedBBox()
	return bbox.Min.X, bbox.Min.Y, bbox.Max.X, bbox.Max.Y
}

func (i *itemT) Point() (x, y float64) {
	x, y, _, _ = i.Rect()
	return
}

// Collection represents a collection of geojson objects.
type Collection struct {
	items    *btree.BTree
	index    *index.Index
	fieldMap map[string]int
	weight   int
	points   int
	objects  int
}

var counter uint64

// New creates an empty collection
func New() *Collection {
	col := &Collection{
		index:    index.New(),
		items:    btree.New(16),
		fieldMap: make(map[string]int),
	}
	return col
}

// Count returns the number of objects in collection.
func (c *Collection) Count() int {
	return c.objects
}

// PointCount returns the number of points (lat/lon coordinates) in collection.
func (c *Collection) PointCount() int {
	return c.points
}

// TotalWeight calculates the in-memory cost of the collection in bytes.
func (c *Collection) TotalWeight() int {
	return c.weight + c.overheadWeight()
}

func (c *Collection) overheadWeight() int {
	// the field map.
	mapweight := 0
	for field := range c.fieldMap {
		mapweight += len(field) + 8 // key + value
	}
	mapweight = int((float64(mapweight) * 1.05) + 28.0) // about an 8% pad plus golang 28 byte map overhead.
	// the btree. each object takes up 64bits for the interface head for each item.
	btreeweight := (c.objects * 8)
	// plus roughly one pointer for every item
	btreeweight += (c.objects * 8)
	// also the btree header weight
	btreeweight += 24
	return mapweight + btreeweight
}

// ReplaceOrInsert adds or replaces an object in the collection and returns the fields array.
// If an item with the same id is already in the collection then the new item will adopt the old item's fields.
// The fields argument is optional.
// The return values are the old object, the old fields, and the new fields
func (c *Collection) ReplaceOrInsert(id string, obj geojson.Object, fields []string, values []float64) (oldObject geojson.Object, oldFields []float64, newFields []float64) {
	oldItem, ok := c.remove(id)
	nitem := c.insert(id, obj)
	if ok {
		oldObject = oldItem.Object
		oldFields = oldItem.Fields
		nitem.Fields = oldFields
		c.weight += len(nitem.Fields) * 8
	}
	for i, field := range fields {
		c.setField(nitem, field, values[i])
	}
	return oldObject, oldFields, nitem.Fields
}

func (c *Collection) remove(id string) (item *itemT, ok bool) {
	i := c.items.Delete(&itemT{ID: id})
	if i == nil {
		return nil, false
	}
	item = i.(*itemT)
	c.index.Remove(item)
	c.weight -= len(item.Fields) * 8
	c.weight -= item.Object.Weight() + len(item.ID)
	c.points -= item.Object.PositionCount()
	c.objects--
	return item, true
}

func (c *Collection) insert(id string, obj geojson.Object) (item *itemT) {
	item = &itemT{ID: id, Object: obj}
	c.index.Insert(item)
	c.items.ReplaceOrInsert(item)
	c.weight += obj.Weight() + len(id)
	c.points += obj.PositionCount()
	c.objects++
	return item
}

// Remove removes an object and returns it.
// If the object does not exist then the 'ok' return value will be false.
func (c *Collection) Remove(id string) (obj geojson.Object, fields []float64, ok bool) {
	item, ok := c.remove(id)
	if !ok {
		return nil, nil, false
	}
	return item.Object, item.Fields, true
}

func (c *Collection) get(id string) (obj geojson.Object, fields []float64, ok bool) {
	i := c.items.Get(&itemT{ID: id})
	if i == nil {
		return nil, nil, false
	}
	item := i.(*itemT)
	return item.Object, item.Fields, true
}

// Get returns an object.
// If the object does not exist then the 'ok' return value will be false.
func (c *Collection) Get(id string) (obj geojson.Object, fields []float64, ok bool) {
	return c.get(id)
}

// SetField set a field value for an object and returns that object.
// If the object does not exist then the 'ok' return value will be false.
func (c *Collection) SetField(id, field string, value float64) (obj geojson.Object, fields []float64, updated bool, ok bool) {
	i := c.items.Get(&itemT{ID: id})
	if i == nil {
		ok = false
		return
	}
	item := i.(*itemT)
	updated = c.setField(item, field, value)
	return item.Object, item.Fields, updated, true
}

func (c *Collection) setField(item *itemT, field string, value float64) (updated bool) {
	idx, ok := c.fieldMap[field]
	if !ok {
		idx = len(c.fieldMap)
		c.fieldMap[field] = idx
	}
	c.weight -= len(item.Fields) * 8
	for idx >= len(item.Fields) {
		item.Fields = append(item.Fields, math.NaN())
	}
	c.weight += len(item.Fields) * 8
	ovalue := item.Fields[idx]
	if math.IsNaN(ovalue) {
		ovalue = 0
	}
	item.Fields[idx] = value
	return ovalue != value
}

// FieldMap return a maps of the field names.
func (c *Collection) FieldMap() map[string]int {
	return c.fieldMap
}

// FieldArr return an array representation of the field names.
func (c *Collection) FieldArr() []string {
	arr := make([]string, len(c.fieldMap))
	for field, i := range c.fieldMap {
		arr[i] = field
	}
	return arr
}

// Scan iterates though the collection. A cursor can be used for paging.
func (c *Collection) Scan(cursor uint64, iterator func(id string, obj geojson.Object, fields []float64) bool) (ncursor uint64) {
	var i uint64
	var active = true
	c.items.Ascend(func(item btree.Item) bool {
		if i >= cursor {
			iitm := item.(*itemT)
			active = iterator(iitm.ID, iitm.Object, iitm.Fields)
		}
		i++
		return active
	})
	return i
}

// ScanGreaterOrEqual iterates though the collection starting with specified id. A cursor can be used for paging.
func (c *Collection) ScanGreaterOrEqual(id string, cursor uint64, iterator func(id string, obj geojson.Object, fields []float64) bool) (ncursor uint64) {
	var i uint64
	var active = true
	c.items.AscendGreaterOrEqual(&itemT{ID: id}, func(item btree.Item) bool {
		if i >= cursor {
			iitm := item.(*itemT)
			active = iterator(iitm.ID, iitm.Object, iitm.Fields)
		}
		i++
		return active
	})
	return i
}

func (c *Collection) search(cursor uint64, bbox geojson.BBox, iterator func(id string, obj geojson.Object, fields []float64) bool) (ncursor uint64) {
	return c.index.Search(cursor, bbox.Min.Y, bbox.Min.X, bbox.Max.Y, bbox.Max.X, func(item index.Item) bool {
		var iitm *itemT
		iitm, ok := item.(*itemT)
		if !ok {
			return true // just ignore
		}
		if !iterator(iitm.ID, iitm.Object, iitm.Fields) {
			return false
		}
		return true
	})
}

// Nearby returns all object that are nearby a point.
func (c *Collection) Nearby(cursor uint64, sparse uint8, lat, lon, meters float64, iterator func(id string, obj geojson.Object, fields []float64) bool) (ncursor uint64) {
	center := geojson.Position{X: lon, Y: lat, Z: 0}
	bbox := geojson.BBoxesFromCenter(lat, lon, meters)
	bboxes := bbox.Sparse(sparse)
	if sparse > 0 {
		for _, bbox := range bboxes {
			c.search(cursor, bbox, func(id string, obj geojson.Object, fields []float64) bool {
				if obj.Nearby(center, meters) {
					if iterator(id, obj, fields) {
						return false
					}
				}
				return true
			})
		}
		return 0
	}
	return c.search(cursor, bbox, func(id string, obj geojson.Object, fields []float64) bool {
		if obj.Nearby(center, meters) {
			return iterator(id, obj, fields)
		}
		return true
	})
}

// Within returns all object that are fully contained within an object or bounding box. Set obj to nil in order to use the bounding box.
func (c *Collection) Within(cursor uint64, sparse uint8, obj geojson.Object, minLat, minLon, maxLat, maxLon float64, iterator func(id string, obj geojson.Object, fields []float64) bool) (ncursor uint64) {
	var bbox geojson.BBox
	if obj != nil {
		bbox = obj.CalculatedBBox()
	} else {
		bbox = geojson.BBox{Min: geojson.Position{X: minLon, Y: minLat, Z: 0}, Max: geojson.Position{X: maxLon, Y: maxLat, Z: 0}}
	}
	bboxes := bbox.Sparse(sparse)
	if sparse > 0 {
		for _, bbox := range bboxes {
			if obj != nil {
				c.search(cursor, bbox, func(id string, o geojson.Object, fields []float64) bool {
					if o.Within(obj) {
						if iterator(id, o, fields) {
							return false
						}
					}
					return true
				})
			}
			c.search(cursor, bbox, func(id string, o geojson.Object, fields []float64) bool {
				if o.WithinBBox(bbox) {
					if iterator(id, o, fields) {
						return false
					}
				}
				return true
			})
		}
		return 0
	}
	if obj != nil {
		return c.search(cursor, bbox, func(id string, o geojson.Object, fields []float64) bool {
			if o.Within(obj) {
				return iterator(id, o, fields)
			}
			return true
		})
	}
	return c.search(cursor, bbox, func(id string, o geojson.Object, fields []float64) bool {
		if o.WithinBBox(bbox) {
			return iterator(id, o, fields)
		}
		return true
	})
}

// Intersects returns all object that are intersect an object or bounding box. Set obj to nil in order to use the bounding box.
func (c *Collection) Intersects(cursor uint64, sparse uint8, obj geojson.Object, minLat, minLon, maxLat, maxLon float64, iterator func(id string, obj geojson.Object, fields []float64) bool) (ncursor uint64) {
	var bbox geojson.BBox
	if obj != nil {
		bbox = obj.CalculatedBBox()
	} else {
		bbox = geojson.BBox{Min: geojson.Position{X: minLon, Y: minLat, Z: 0}, Max: geojson.Position{X: maxLon, Y: maxLat, Z: 0}}
	}
	var bboxes []geojson.BBox
	if sparse > 0 {
		split := 1 << sparse
		xpart := (bbox.Max.X - bbox.Min.X) / float64(split)
		ypart := (bbox.Max.Y - bbox.Min.Y) / float64(split)
		for y := bbox.Min.Y; y < bbox.Max.Y; y += ypart {
			for x := bbox.Min.X; x < bbox.Max.X; x += xpart {
				bboxes = append(bboxes, geojson.BBox{
					Min: geojson.Position{X: x, Y: y, Z: 0},
					Max: geojson.Position{X: x + xpart, Y: y + ypart, Z: 0},
				})
			}
		}
		for _, bbox := range bboxes {
			if obj != nil {
				c.search(cursor, bbox, func(id string, o geojson.Object, fields []float64) bool {
					if o.Intersects(obj) {
						if iterator(id, o, fields) {
							return false
						}
					}
					return true
				})
			}
			c.search(cursor, bbox, func(id string, o geojson.Object, fields []float64) bool {
				if o.IntersectsBBox(bbox) {
					if iterator(id, o, fields) {
						return false
					}
				}
				return true
			})
		}
		return 0
	}
	if obj != nil {
		return c.search(cursor, bbox, func(id string, o geojson.Object, fields []float64) bool {
			if o.Intersects(obj) {
				return iterator(id, o, fields)
			}
			return true
		})
	}
	return c.search(cursor, bbox, func(id string, o geojson.Object, fields []float64) bool {
		if o.IntersectsBBox(bbox) {
			return iterator(id, o, fields)
		}
		return true
	})
}
