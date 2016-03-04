// +build ignore

package core

import (
	"encoding/json"
	"strings"
)

const (
	clear  = "\x1b[0m"
	bright = "\x1b[1m"
	gray   = "\x1b[90m"
	yellow = "\x1b[33m"
)

type Command struct {
	Name       string     `json:"-"`
	Summary    string     `json:"summary"`
	Complexity string     `json:"complexity"`
	Arguments  []Argument `json:"arguments"`
	Since      string     `json:"since"`
	Group      string     `json:"group"`
	DevOnly    bool       `json:"dev"`
}

func (c Command) String() string {
	var s = c.Name
	for _, arg := range c.Arguments {
		s += " " + arg.String()
	}
	return s
}

func (c Command) TermOutput(indent string) string {
	line1 := bright + strings.Replace(c.String(), " ", " "+clear+gray, 1) + clear
	line2 := yellow + "summary: " + clear + c.Summary
	line3 := yellow + "since: " + clear + c.Since
	return indent + line1 + "\n" + indent + line2 + "\n" + indent + line3 + "\n"
}

type EnumArg struct {
	Name      string     `json:"name"`
	Arguments []Argument `json:"arguments`
}

func (a EnumArg) String() string {
	var s = a.Name
	for _, arg := range a.Arguments {
		s += " " + arg.String()
	}
	return s
}

type Argument struct {
	Command  string      `json:"command"`
	NameAny  interface{} `json:"name"`
	TypeAny  interface{} `json:"type"`
	Optional bool        `json:"optional"`
	Multiple bool        `json:"multiple"`
	Variadic bool        `json:"variadic"`
	Enum     []string    `json:"enum"`
	EnumArgs []EnumArg   `json:"enumargs"`
}

func (a Argument) String() string {
	var s string
	if a.Command != "" {
		s += " " + a.Command
	}
	if len(a.EnumArgs) > 0 {
		eargs := ""
		for _, arg := range a.EnumArgs {
			v := arg.String()
			if strings.Contains(v, " ") {
				v = "(" + v + ")"
			}
			eargs += v + "|"
		}
		if len(eargs) > 0 {
			eargs = eargs[:len(eargs)-1]
		}
		s += " " + eargs
	} else if len(a.Enum) > 0 {
		s += " " + strings.Join(a.Enum, "|")
	} else {
		names, _ := a.NameTypes()
		subs := ""
		for _, name := range names {
			subs += " " + name
		}
		subs = strings.TrimSpace(subs)
		s += " " + subs
		if a.Variadic {
			s += " [" + subs + " ...]"
		}
		if a.Multiple {
			s += " ..."
		}
	}
	s = strings.TrimSpace(s)
	if a.Optional {
		s = "[" + s + "]"
	}
	return s
}

func parseAnyStringArray(any interface{}) []string {
	if str, ok := any.(string); ok {
		return []string{str}
	} else if any, ok := any.([]interface{}); ok {
		arr := []string{}
		for _, any := range any {
			if str, ok := any.(string); ok {
				arr = append(arr, str)
			}
		}
		return arr
	}
	return []string{}
}

func (a Argument) NameTypes() (names, types []string) {
	names = parseAnyStringArray(a.NameAny)
	types = parseAnyStringArray(a.TypeAny)
	if len(types) > len(names) {
		types = types[:len(names)]
	} else {
		for len(types) < len(names) {
			types = append(types, "")
		}
	}
	return
}

var Commands = func() map[string]Command {
	var commands map[string]Command
	if err := json.Unmarshal([]byte(commandsJSON), &commands); err != nil {
		panic(err.Error())
	}
	for name, command := range commands {
		command.Name = strings.ToUpper(name)
		commands[name] = command
	}
	return commands
}()

var commandsJSON = `{{.CommandsJSON}}`

// --- Crud ---
// SET         key id [FIELD name value ...] @input                            -- Sets the object of an id. -- O(1) -- F(key string, id string, name string, value double)
// FSET        key id name value                                               -- Set a single field of an id. -- O(1) -- F(key string, id string, value double)
// GET         key id [OBJECT | POINT | BOUNDS | HASH precision]               -- Get the object of an id. -- O(1) -- F(key string, id string)
// DEL         key id                                                          -- Delete an id. -- O(1) -- F(key string, id string)
// DROP        key                                                             -- Drops a key. -- O(1) -- F(key string)
// KEYS        pattern                                                         -- Finds all keys matching the given pattern. -- O(N) where N is the number of keys in the database -- F(pattern pattern)
// STATS       key [key ...]                                                   -- Show stats for one or more keys. -- O(N) where N is the number of keys being requested -- F(key string)

// --- Search ---
// SCAN        key @options @output                                            -- Incrementally iterate though a key. Some options are not allowed such as FENCE and SPARCE. -- O(N) where N is the number of ids in the key -- F(key string)
// NEARBY      key @options @output POINT lat lon meters                       -- Searches for ids that are nearby a point. -- O(log(N)) where N is the number of ids in the area -- F(key string, lat double, lon double, meters double)
// WITHIN      key @options @output @area                                      -- Searches for ids that are fullly contained inside area. -- O(log(N)) where N is the number of ids in the area -- F(key string)
// INTERSECTS  key @options @output @area                                      -- Searches for ids that intersect an area. -- O(log(N)) where N is the number of ids in the area -- F(key string)

// --- Server ---
// PING                                                                        -- Ping the server. -- O(1) -- F()
// SERVER                                                                      -- Show server stats and details. -- O(1) -- F()
// GC                                                                          -- Forces a garbage collection. -- O(1) -- F()
// HELP                                                                        -- Prints this menu. -- O(1) -- F()
// READONLY    value                                                           -- Turn on or off readonly mode. -- O(1) -- F(value boolean)
// FLUSHDB                                                                     -- Removes all keys. -- O(1) -- F()

// --- Replication ---
// FOLLOW      host port                                                       -- Follows a leader host. -- O(1) F(host string, port integer)
// AOF         pos                                                             -- Downloads an aof starting from pos. The connection does not close and continues to load data. -- O(1) F(pos integer)
// AOFMD5      pos size                                                        -- Does a checksum on a portion of the aof. -- O(1) F(pos integer, size integer)
// AOFSHRINK                                                                   -- Shrinks the aof in the background. -- O(1) F()

// --- Dev ---
// MASSINSERT  keys count                                                      -- Randomly inserts objects in to specified number of key. -- O(N) where N is the number keys being created -- F(keys integer, count integer) -- !dev only!

// --- Parameters ---
// @input      (OBJECT... | POINT... | BOUNDS... | HASH...)
// @options    [CURSOR...] [LIMIT...] [SPARSE...] [MATCH...] [WHERE...] [NOFIELDS] [FENCE]
// @output     [COUNT | IDS | OBJECTS | POINTS | BOUNDS | HASHES...]
// @area       [GET... | BOUNDS... | OBJECT... | TILE... | QUADKEY... | HASH precision]
// FENCE                                                                       -- Opens a live fence. -- F()
// FIELD...    FIELD name value                                                -- A name and value of a field. -- F(name string, value double)
// CURSOR...   CURSOR start                                                    -- Value indicating the starting position of the next page of data. -- F(start integer)
// NOFIELDS    NOFIELDS                                                        -- Do not return fields with the results. -- F()
// LIMIT...    LIMIT count                                                     -- Limit the number of items returned. default is 100. -- F(count integer)
// WHERE...    WHERE field min max                                             -- Filters results where a field is between min and max. http://redis.io/commands/ZRANGEBYSCORE. -- F(field string, min integer, max integer)
// MATCH...    MATCH pattern                                                   -- Filters results where the id matched the pattern. -- F(pattern pattern)
// BOUNDS...   BOUNDS minlat minlon maxlat maxlon                              -- Bounding box. -- F(minlat double, minlon double, maxlat double, maxlon double)
// OBJECT...   OBJECT object                                                   -- Valid GeoJSON object. -- F(object geojson)
// POINT...    POINT lat lon [z]                                               -- Standard coordinate. -- F(lat double, lon double, z double)
// HASHES...   HASHES precision                                                -- Geohash. The precision must between 1 and 22. -- F(precision double)
// HASH...     HASH hash                                                       -- Geohash. -- F(hash geohash)
// QUADKEY...  QUADKEY key                                                     -- Quadkey. -- F(key quadkey)
// TILE...     TILE x y z                                                      -- Google XYZ tile. -- F(x double, y double, z double)
// GET...      GET key id                                                      -- An internal object. -- F(key string, id string)
