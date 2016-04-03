// Package rtree - A 2d Implementation of RTree, a bounding rectangle tree.
//
// This file is derived from the work done by Toni Gutman. R-Trees: A Dynamic Index Structure for
// Spatial Searching, Proc. 1984 ACM SIGMOD International Conference on Management of Data, pp.
// 47-57. The implementation found in SQLite is a refinement of Guttman's original idea, commonly
// called "R*Trees", that was described by Norbert Beckmann, Hans-Peter Kriegel, Ralf Schneider,
// Bernhard Seeger: The R*-Tree: An Efficient and Robust Access Method for Points and Rectangles.
// SIGMOD Conference 1990: 322-331
//
// The original C code can be found at "http://www.superliminal.com/sources/sources.htm".
//
// And the website carries this message: "Here are a few useful bits of free source code. You're
// completely free to use them for any purpose whatsoever. All I ask is that if you find one to
// be particularly valuable, then consider sending feedback. Please send bugs and suggestions too.
// Enjoy"
package rtree

import "math"

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

func min(a, b float64) float64 {
	if a < b {
		return a
	}
	return b
}
func max(a, b float64) float64 {
	if a > b {
		return a
	}
	return b
}

const (
	unitSphereVolume1 = 2.000000
	unitSphereVolume2 = 3.141593
	unitSphereVolume3 = 4.188790
	unitSphereVolume4 = 4.934802
)
const (
	maxNodes           = 16
	minNodes           = maxNodes / 2
	useSphericalVolume = true
	unitSphereVolume   = unitSphereVolume2
)

/// Minimal bounding rectangle (n-dimensional)
type rectT struct {
	min [2]float64 ///< Min dimensions of bounding box
	max [2]float64 ///< Max dimensions of bounding box
}

/// May be data or may be another subtree
/// The parents level determines this.
/// If the parents level is 0, then this is data
type branchT struct {
	rect  rectT  ///< Bounds
	child *nodeT ///< Child node
	item  Item   ///< Data ID or Ptr
}

/// nodeT for each branch level
type nodeT struct {
	count  int               ///< Count
	level  int               ///< Leaf is zero, others positive
	branch [maxNodes]branchT ///< branchT
}

func (node *nodeT) isInternalNode() bool { return node.level > 0 } // Not a leaf, but a internal node

/// A link list of nodes for reinsertion after a delete operation
type listNodeT struct {
	next *listNodeT ///< Next in list
	node *nodeT     ///< nodeT
}

/// Variables for finding a split partition
type partitionVarsT struct {
	partition      [maxNodes + 1]int
	total          int
	minFill        int
	taken          [maxNodes + 1]bool
	count          [2]int
	cover          [2]rectT
	area           [2]float64
	branchBuf      [maxNodes + 1]branchT
	branchCount    int
	coverSplit     rectT
	coverSplitArea float64
}

// RTree is an implementation of an rtree
type RTree struct {
	root *nodeT
}

func itemRect(item Item) (rect rectT) {
	minX, minY, maxX, maxY := item.Rect()
	return rectT{
		min: [2]float64{minX, minY},
		max: [2]float64{maxX, maxY},
	}
}

// New creates a new RTree
func New() *RTree {
	return &RTree{}
}

// Insert inserts item into rtree
func (tr *RTree) Insert(item Item) {
	if tr.root == nil {
		tr.root = &nodeT{}
	}
	insertRect(itemRect(item), item, &tr.root, 0)
}

// Remove removes item from rtree
func (tr *RTree) Remove(item Item) {
	if tr.root == nil {
		tr.root = &nodeT{}
	}
	removeRect(itemRect(item), item, &tr.root)
}

// Search finds all items in bounding box.
func (tr *RTree) Search(minX, minY, maxX, maxY float64, iterator func(item Item) bool) {
	if iterator == nil {
		return
	}
	rect := rectT{
		min: [2]float64{minX, minY},
		max: [2]float64{maxX, maxY},
	}
	// NOTE: May want to return search result another way, perhaps returning the number of found elements here.
	if tr.root == nil {
		tr.root = &nodeT{}
	}
	search(tr.root, rect, iterator)
}

// Count return the number of items in rtree.
func (tr *RTree) Count() int {
	return countRec(tr.root, 0)
}

// RemoveAll removes all items from rtree.
func (tr *RTree) RemoveAll() {
	tr.root = nil
}

func countRec(node *nodeT, counter int) int {
	if node.isInternalNode() { // not a leaf node
		for index := 0; index < node.count; index++ {
			counter = countRec(node.branch[index].child, counter)
		}
	} else { // A leaf node
		if node.count > 256 {
			println(node.count)
		}
		counter += node.count
	}
	return counter
}

// Inserts a new data rectangle into the index structure.
// Recursively descends tree, propagates splits back up.
// Returns 0 if node was not split.  Old node updated.
// If node was split, returns 1 and sets the pointer pointed to by
// new_node to point to the new node.  Old node updated to become one of two.
// The level argument specifies the number of steps up from the leaf
// level to insert; e.g. a data rectangle goes in at level = 0.
func insertRectRec(rect rectT, item Item, node *nodeT, newNode **nodeT, level int) bool {
	var index int
	var branch branchT
	var otherNode *nodeT
	// Still above level for insertion, go down tree recursively
	if node.level > level {
		index = pickBranch(rect, node)
		if !insertRectRec(rect, item, node.branch[index].child, &otherNode, level) {
			// Child was not split
			node.branch[index].rect = combineRect(rect, node.branch[index].rect)
			return false
		} // Child was split
		node.branch[index].rect = nodeCover(node.branch[index].child)
		branch.child = otherNode
		branch.rect = nodeCover(otherNode)
		return addBranch(&branch, node, newNode)
	} else if node.level == level { // Have reached level for insertion. Add rect, split if necessary
		branch.rect = rect
		branch.item = item
		// Child field of leaves contains id of data record
		return addBranch(&branch, node, newNode)
	} else {
		// Should never occur
		return false
	}
}

// Insert a data rectangle into an index structure.
// InsertRect provides for splitting the root;
// returns 1 if root was split, 0 if it was not.
// The level argument specifies the number of steps up from the leaf
// level to insert; e.g. a data rectangle goes in at level = 0.
// InsertRect2 does the recursion.
func insertRect(rect rectT, item Item, root **nodeT, level int) bool {
	var newRoot *nodeT
	var newNode *nodeT
	var branch branchT
	if insertRectRec(rect, item, *root, &newNode, level) { // Root split
		newRoot = &nodeT{} // Grow tree taller and new root
		newRoot.level = (*root).level + 1
		branch.rect = nodeCover(*root)
		branch.child = *root
		addBranch(&branch, newRoot, nil)
		branch.rect = nodeCover(newNode)
		branch.child = newNode
		addBranch(&branch, newRoot, nil)
		*root = newRoot
		return true
	}
	return false
}

// Find the smallest rectangle that includes all rectangles in branches of a node.
func nodeCover(node *nodeT) rectT {
	var firstTime = true
	var rect rectT
	for index := 0; index < node.count; index++ {
		if firstTime {
			rect = node.branch[index].rect
			firstTime = false
		} else {
			rect = combineRect(rect, node.branch[index].rect)
		}
	}
	return rect
}

// Add a branch to a node.  Split the node if necessary.
// Returns 0 if node not split.  Old node updated.
// Returns 1 if node split, sets *new_node to address of new node.
// Old node updated, becomes one of two.
func addBranch(branch *branchT, node *nodeT, newNode **nodeT) bool {
	if node.count < maxNodes { // Split won't be necessary
		node.branch[node.count] = *branch
		node.count++
		return false
	}
	splitNode(node, branch, newNode)
	return true
}

// Disconnect a dependent node.
// Caller must return (or stop using iteration index) after this as count has changed
func disconnectBranch(node *nodeT, index int) {
	// Remove element by swapping with the last element to prevent gaps in array
	node.branch[index] = node.branch[node.count-1]
	node.count--
}

// Pick a branch.  Pick the one that will need the smallest increase
// in area to accommodate the new rectangle.  This will result in the
// least total area for the covering rectangles in the current node.
// In case of a tie, pick the one which was smaller before, to get
// the best resolution when searching.
func pickBranch(rect rectT, node *nodeT) int {
	var firstTime = true
	var increase float64
	var bestIncr float64 = -1
	var area float64
	var bestArea float64
	var best int
	var tempRect rectT
	for index := 0; index < node.count; index++ {
		curRect := node.branch[index].rect
		area = calcRectVolume(curRect)
		tempRect = combineRect(rect, curRect)
		increase = calcRectVolume(tempRect) - area
		if (increase < bestIncr) || firstTime {
			best = index
			bestArea = area
			bestIncr = increase
			firstTime = false
		} else if (increase == bestIncr) && (area < bestArea) {
			best = index
			bestArea = area
			bestIncr = increase
		}
	}
	return best
}

// Combine two rectangles into larger one containing both
func combineRect(rectA, rectB rectT) rectT {
	var newRect rectT
	for index := 0; index < 2; index++ {
		newRect.min[index] = min(rectA.min[index], rectB.min[index])
		newRect.max[index] = max(rectA.max[index], rectB.max[index])
	}
	return newRect
}

// Split a node.
// Divides the nodes branches and the extra one between two nodes.
// Old node is one of the new ones, and one really new one is created.
// Tries more than one method for choosing a partition, uses best result.

func splitNode(node *nodeT, branch *branchT, newNode **nodeT) {
	// Could just use local here, but member or external is faster since it is reused
	var localVars partitionVarsT
	var parVars = &localVars
	var level int
	// Load all the branches into a buffer, initialize old node
	level = node.level
	getBranches(node, branch, parVars)
	// Find partition
	choosePartition(parVars, minNodes)
	// Put branches from buffer into 2 nodes according to chosen partition
	*newNode = &nodeT{}
	node.level = level
	(*newNode).level = node.level
	loadNodes(node, *newNode, parVars)
}

// Calculate the n-dimensional volume of a rectangle
func rectVolume(rect rectT) float64 {
	var volume float64 = 1
	for index := 0; index < 2; index++ {
		volume *= rect.max[index] - rect.min[index]
	}
	return volume
}

// The exact volume of the bounding sphere for the given rectT
func rectSphericalVolume(rect rectT) float64 {
	var sumOfSquares float64
	var radius float64
	for index := 0; index < 2; index++ {
		var halfExtent = (rect.max[index] - rect.min[index]) * 0.5
		sumOfSquares += halfExtent * halfExtent
	}
	radius = math.Sqrt(sumOfSquares)
	// Pow maybe slow, so test for common dims like 2,3 and just use x*x, x*x*x.
	if 2 == 3 {
		return radius * radius * radius * unitSphereVolume
	} else if 2 == 2 {
		return radius * radius * unitSphereVolume
	} else {
		return math.Pow(radius, 2) * unitSphereVolume
	}
}

// Use one of the methods to calculate retangle volume
func calcRectVolume(rect rectT) float64 {
	if useSphericalVolume {
		return rectSphericalVolume(rect) // Slower but helps certain merge cases
	}
	return rectVolume(rect) // Faster but can cause poor merges
}

// Load branch buffer with branches from full node plus the extra branch.
func getBranches(node *nodeT, branch *branchT, parVars *partitionVarsT) {
	// Load the branch buffer
	for index := 0; index < maxNodes; index++ {
		parVars.branchBuf[index] = node.branch[index]
	}
	parVars.branchBuf[maxNodes] = *branch
	parVars.branchCount = maxNodes + 1
	// Calculate rect containing all in the set
	parVars.coverSplit = parVars.branchBuf[0].rect
	for index := 1; index < maxNodes+1; index++ {
		parVars.coverSplit = combineRect(parVars.coverSplit, parVars.branchBuf[index].rect)
	}
	parVars.coverSplitArea = calcRectVolume(parVars.coverSplit)
	node.count = 0
	node.level = -1
}

// Method #0 for choosing a partition:
// As the seeds for the two groups, pick the two rects that would waste the
// most area if covered by a single rectangle, i.e. evidently the worst pair
// to have in the same group.
// Of the remaining, one at a time is chosen to be put in one of the two groups.
// The one chosen is the one with the greatest difference in area expansion
// depending on which group - the rect most strongly attracted to one group
// and repelled from the other.
// If one group gets too full (more would force other group to violate min
// fill requirement) then other group gets the rest.
// These last are the ones that can go in either group most easily.
func choosePartition(parVars *partitionVarsT, minFill int) {
	var biggestDiff float64
	var group, chosen, betterGroup int
	initParVars(parVars, parVars.branchCount, minFill)
	pickSeeds(parVars)
	for ((parVars.count[0] + parVars.count[1]) < parVars.total) &&
		(parVars.count[0] < (parVars.total - parVars.minFill)) &&
		(parVars.count[1] < (parVars.total - parVars.minFill)) {
		biggestDiff = -1
		for index := 0; index < parVars.total; index++ {
			if !parVars.taken[index] {
				var curRect = parVars.branchBuf[index].rect
				rect0 := combineRect(curRect, parVars.cover[0])
				rect1 := combineRect(curRect, parVars.cover[1])
				growth0 := calcRectVolume(rect0) - parVars.area[0]
				growth1 := calcRectVolume(rect1) - parVars.area[1]
				diff := growth1 - growth0
				if diff >= 0 {
					group = 0
				} else {
					group = 1
					diff = -diff
				}
				if diff > biggestDiff {
					biggestDiff = diff
					chosen = index
					betterGroup = group
				} else if (diff == biggestDiff) && (parVars.count[group] < parVars.count[betterGroup]) {
					chosen = index
					betterGroup = group
				}
			}
		}
		classify(chosen, betterGroup, parVars)
	}
	// If one group too full, put remaining rects in the other
	if (parVars.count[0] + parVars.count[1]) < parVars.total {
		if parVars.count[0] >= parVars.total-parVars.minFill {
			group = 1
		} else {
			group = 0
		}
		for index := 0; index < parVars.total; index++ {
			if !parVars.taken[index] {
				classify(index, group, parVars)
			}
		}
	}
}

// Copy branches from the buffer into two nodes according to the partition.
func loadNodes(nodeA *nodeT, nodeB *nodeT, parVars *partitionVarsT) {
	for index := 0; index < parVars.total; index++ {
		if parVars.partition[index] == 0 {
			addBranch(&parVars.branchBuf[index], nodeA, nil)
		} else if parVars.partition[index] == 1 {
			addBranch(&parVars.branchBuf[index], nodeB, nil)
		}
	}
}

// Initialize a partitionVarsT structure.
func initParVars(parVars *partitionVarsT, maxRects int, minFill int) {
	parVars.count[1] = 0
	parVars.count[0] = parVars.count[1]
	parVars.area[1] = 0
	parVars.area[0] = parVars.area[1]
	parVars.total = maxRects
	parVars.minFill = minFill
	for index := 0; index < maxRects; index++ {
		parVars.taken[index] = false
		parVars.partition[index] = -1
	}
}

func pickSeeds(parVars *partitionVarsT) {
	var seed0, seed1 int
	var worst, waste float64
	var area [maxNodes + 1]float64
	for index := 0; index < parVars.total; index++ {
		area[index] = calcRectVolume(parVars.branchBuf[index].rect)
	}
	worst = -parVars.coverSplitArea - 1
	for indexA := 0; indexA < parVars.total-1; indexA++ {
		for indexB := indexA + 1; indexB < parVars.total; indexB++ {
			var oneRect = combineRect(parVars.branchBuf[indexA].rect, parVars.branchBuf[indexB].rect)
			waste = calcRectVolume(oneRect) - area[indexA] - area[indexB]
			if waste > worst {
				worst = waste
				seed0 = indexA
				seed1 = indexB
			}
		}
	}
	classify(seed0, 0, parVars)
	classify(seed1, 1, parVars)
}

// Put a branch in one of the groups.
func classify(index int, group int, parVars *partitionVarsT) {
	parVars.partition[index] = group
	parVars.taken[index] = true

	if parVars.count[group] == 0 {
		parVars.cover[group] = parVars.branchBuf[index].rect
	} else {
		parVars.cover[group] = combineRect(parVars.branchBuf[index].rect, parVars.cover[group])
	}
	parVars.area[group] = calcRectVolume(parVars.cover[group])
	parVars.count[group]++
}

// Delete a data rectangle from an index structure.
// Pass in a pointer to a rectT, the tid of the record, ptr to ptr to root node.
// Returns 1 if record not found, 0 if success.
// RemoveRect provides for eliminating the root.
func removeRect(rect rectT, item Item, root **nodeT) bool {
	var tempNode *nodeT
	var reInsertList *listNodeT
	if !removeRectRec(rect, item, *root, &reInsertList) {
		// Found and deleted a data item
		// Reinsert any branches from eliminated nodes
		for reInsertList != nil {
			tempNode = reInsertList.node
			for index := 0; index < tempNode.count; index++ {
				insertRect(tempNode.branch[index].rect,
					tempNode.branch[index].item,
					root,
					tempNode.level)
			}
			reInsertList = reInsertList.next
		}
		// Check for redundant root (not leaf, 1 child) and eliminate
		if (*root).count == 1 && (*root).isInternalNode() {
			tempNode = (*root).branch[0].child
			*root = tempNode
		}
		return false
	}
	return true
}

// Delete a rectangle from non-root part of an index structure.
// Called by RemoveRect.  Descends tree recursively,
// merges branches on the way back up.
// Returns 1 if record not found, 0 if success.
func removeRectRec(rect rectT, item Item, node *nodeT, listNode **listNodeT) bool {
	if node.isInternalNode() { // not a leaf node
		for index := 0; index < node.count; index++ {
			if overlap(rect, node.branch[index].rect) {
				if !removeRectRec(rect, item, node.branch[index].child, listNode) {
					if node.branch[index].child.count >= minNodes {
						// child removed, just resize parent rect
						node.branch[index].rect = nodeCover(node.branch[index].child)
					} else {
						// child removed, not enough entries in node, eliminate node
						reInsert(node.branch[index].child, listNode)
						disconnectBranch(node, index) // Must return after this call as count has changed
					}
					return false
				}
			}
		}
		return true
	}
	// A leaf node
	for index := 0; index < node.count; index++ {
		if node.branch[index].item == item {
			disconnectBranch(node, index) // Must return after this call as count has changed
			return false
		}
	}
	return true
}

// Decide whether two rectangles overlap.
func overlap(rectA rectT, rectB rectT) bool {
	for index := 0; index < 2; index++ {
		if rectA.min[index] > rectB.max[index] ||
			rectB.min[index] > rectA.max[index] {
			return false
		}
	}
	return true
}

// Add a node to the reinsertion list.  All its branches will later
// be reinserted into the index structure.
func reInsert(node *nodeT, listNode **listNodeT) {
	*listNode = &listNodeT{
		node: node,
		next: *listNode,
	}
}

// Search in an index tree or subtree for all data retangles that overlap the argument rectangle.
func search(node *nodeT, rect rectT, iterator func(item Item) bool) bool {
	if node.isInternalNode() { // This is an internal node in the tree
		for index := 0; index < node.count; index++ {
			nrect := node.branch[index].rect
			if overlap(rect, nrect) {
				if !search(node.branch[index].child, rect, iterator) {
					return false // Don't continue searching
				}
			}
		}
	} else { // This is a leaf node
		for index := 0; index < node.count; index++ {
			if overlap(rect, node.branch[index].rect) {
				// NOTE: There are different ways to return results.  Here's where to modify
				if !iterator(node.branch[index].item) {
					return false // Don't continue searching
				}
			}
		}
	}
	return true // Continue searching
}
