package engine

import (
	"errors"
	"math"
	"sort"
)

const geoMaxLatitude = 85.05112878
const earthRadiusMeters = 6372797.560856
const geoAlphabet = "0123456789bcdefghjkmnpqrstuvwxyz"

var ErrInvalidGeo = errors.New("invalid longitude, latitude, or distance")

type GeoPoint struct {
	Longitude, Latitude float64
	Member              string
}
type GeoResult struct {
	Member                        string
	Longitude, Latitude, Distance float64
	Hash                          uint64
}

func validGeo(point GeoPoint) bool {
	return !math.IsNaN(point.Longitude) && !math.IsNaN(point.Latitude) && !math.IsInf(point.Longitude, 0) && !math.IsInf(point.Latitude, 0) && point.Longitude >= -180 && point.Longitude <= 180 && point.Latitude >= -geoMaxLatitude && point.Latitude <= geoMaxLatitude
}

func geoScore(longitude, latitude float64) uint64 {
	x := uint64(math.Floor((longitude + 180) / 360 * (1 << 26)))
	y := uint64(math.Floor((latitude + geoMaxLatitude) / (2 * geoMaxLatitude) * (1 << 26)))
	if x >= 1<<26 {
		x = (1 << 26) - 1
	}
	if y >= 1<<26 {
		y = (1 << 26) - 1
	}
	var score uint64
	for i := 25; i >= 0; i-- {
		score = score<<1 | (x >> uint(i) & 1)
		score = score<<1 | (y >> uint(i) & 1)
	}
	return score
}

func geoDecode(score uint64) (float64, float64) {
	var x, y uint64
	for i := 25; i >= 0; i-- {
		x = x<<1 | score>>uint(2*i+1)&1
		y = y<<1 | score>>uint(2*i)&1
	}
	longitude := (float64(x)+0.5)*360/(1<<26) - 180
	latitude := (float64(y)+0.5)*(2*geoMaxLatitude)/(1<<26) - geoMaxLatitude
	return longitude, latitude
}

func geoDistance(lon1, lat1, lon2, lat2 float64) float64 {
	lat1 *= math.Pi / 180
	lat2 *= math.Pi / 180
	dLat := (lat2 - lat1) / 2
	dLon := (lon2 - lon1) * math.Pi / 360
	a := math.Sin(dLat)*math.Sin(dLat) + math.Cos(lat1)*math.Cos(lat2)*math.Sin(dLon)*math.Sin(dLon)
	return 2 * earthRadiusMeters * math.Asin(math.Min(1, math.Sqrt(a)))
}

func geoHash(longitude, latitude float64) string {
	var hash uint64
	loLon, hiLon := -180.0, 180.0
	loLat, hiLat := -90.0, 90.0
	for i := 0; i < 55; i++ {
		hash <<= 1
		if i%2 == 0 {
			mid := (loLon + hiLon) / 2
			if longitude >= mid {
				hash |= 1
				loLon = mid
			} else {
				hiLon = mid
			}
		}
		if i%2 == 1 {
			mid := (loLat + hiLat) / 2
			if latitude >= mid {
				hash |= 1
				loLat = mid
			} else {
				hiLat = mid
			}
		}
	}
	result := make([]byte, 11)
	for i := 10; i >= 0; i-- {
		result[i] = geoAlphabet[hash&31]
		hash >>= 5
	}
	return string(result)
}

func (db *DB) GeoAdd(key string, points []GeoPoint, options ZAddOptions) (ZAddResult, error) {
	items := make([]ZSetItem, len(points))
	for i, point := range points {
		if !validGeo(point) {
			return ZAddResult{}, ErrInvalidGeo
		}
		items[i] = ZSetItem{Member: point.Member, Score: float64(geoScore(point.Longitude, point.Latitude))}
	}
	return db.ZAddMany(key, options, items...)
}

func (db *DB) GeoPos(key string, members ...string) ([]*GeoPoint, error) {
	if db.shards != nil {
		release := db.operationGate()
		defer release()
		return db.shardFor(key).GeoPos(key, members...)
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	db.purgeExpiredLocked()
	result := make([]*GeoPoint, len(members))
	e, found := db.entryLocked(key)
	if !found {
		return result, nil
	}
	if e.kind != KindSortedSet {
		return nil, ErrWrongType
	}
	z := e.value.(*zset)
	for i, member := range members {
		score, found := z.score(member)
		if !found {
			continue
		}
		if score < 0 || score >= 1<<52 || math.Trunc(score) != score {
			return nil, ErrInvalidGeo
		}
		lon, lat := geoDecode(uint64(score))
		result[i] = &GeoPoint{Longitude: lon, Latitude: lat, Member: member}
	}
	return result, nil
}

func (db *DB) GeoDist(key, first, second string) (float64, bool, error) {
	points, err := db.GeoPos(key, first, second)
	if err != nil {
		return 0, false, err
	}
	if points[0] == nil || points[1] == nil {
		return 0, false, nil
	}
	return geoDistance(points[0].Longitude, points[0].Latitude, points[1].Longitude, points[1].Latitude), true, nil
}

func (db *DB) GeoHash(key string, members ...string) ([]*string, error) {
	points, err := db.GeoPos(key, members...)
	if err != nil {
		return nil, err
	}
	result := make([]*string, len(points))
	for i, point := range points {
		if point != nil {
			hash := geoHash(point.Longitude, point.Latitude)
			result[i] = &hash
		}
	}
	return result, nil
}

type GeoSearchOptions struct {
	FromMember            string
	Longitude, Latitude   float64
	UseMember             bool
	Radius, Width, Height float64
	Box                   bool
	Desc                  bool
	Count                 int
}

func (db *DB) GeoSearch(key string, options GeoSearchOptions) ([]GeoResult, error) {
	if options.Radius < 0 || options.Width < 0 || options.Height < 0 || options.Count < 0 {
		return nil, ErrInvalidGeo
	}
	if db.shards != nil {
		release := db.operationGate()
		defer release()
		return db.shardFor(key).GeoSearch(key, options)
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	db.purgeExpiredLocked()
	e, found := db.entryLocked(key)
	if !found {
		return []GeoResult{}, nil
	}
	if e.kind != KindSortedSet {
		return nil, ErrWrongType
	}
	z := e.value.(*zset)
	if options.UseMember {
		score, found := z.score(options.FromMember)
		if !found {
			return nil, ErrNoSuchKey
		}
		if score < 0 || score >= 1<<52 || math.Trunc(score) != score {
			return nil, ErrInvalidGeo
		}
		options.Longitude, options.Latitude = geoDecode(uint64(score))
	} else if !validGeo(GeoPoint{Longitude: options.Longitude, Latitude: options.Latitude}) {
		return nil, ErrInvalidGeo
	}
	result := make([]GeoResult, 0)
	for member, score := range z.dict {
		if score < 0 || score >= 1<<52 || math.Trunc(score) != score {
			return nil, ErrInvalidGeo
		}
		lon, lat := geoDecode(uint64(score))
		distance := geoDistance(options.Longitude, options.Latitude, lon, lat)
		if options.Box {
			vertical := geoDistance(options.Longitude, options.Latitude, options.Longitude, lat)
			horizontal := geoDistance(options.Longitude, options.Latitude, lon, options.Latitude)
			if vertical > options.Height/2 || horizontal > options.Width/2 {
				continue
			}
		} else if distance > options.Radius {
			continue
		}
		result = append(result, GeoResult{Member: member, Longitude: lon, Latitude: lat, Distance: distance, Hash: uint64(score)})
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].Distance == result[j].Distance {
			return result[i].Member < result[j].Member
		}
		if options.Desc {
			return result[i].Distance > result[j].Distance
		}
		return result[i].Distance < result[j].Distance
	})
	if options.Count > 0 && len(result) > options.Count {
		result = result[:options.Count]
	}
	return result, nil
}
