package server

import (
	"errors"
	"math"
	"strconv"
	"strings"

	"kestreldb/internal/engine"
	"kestreldb/internal/proto"
)

func geoUnit(raw string) (float64, error) {
	switch strings.ToLower(raw) {
	case "m":
		return 1, nil
	case "km":
		return 1000, nil
	case "ft":
		return 0.3048, nil
	case "mi":
		return 1609.344, nil
	}
	return 0, errors.New("unsupported unit")
}

func geoFloat(raw string) (float64, error) {
	value, err := strconv.ParseFloat(raw, 64)
	if err != nil || math.IsInf(value, 0) || math.IsNaN(value) {
		return 0, engine.ErrInvalidGeo
	}
	return value, nil
}

func geoNumber(value float64) []byte { return []byte(strconv.FormatFloat(value, 'f', 6, 64)) }

func (h *handler) writeGeoCommand(w *proto.Writer, cmd string, args []string) error {
	key := args[0]
	switch cmd {
	case "GEOADD":
		i := 1
		options := engine.ZAddOptions{}
		for i < len(args) {
			switch strings.ToUpper(args[i]) {
			case "NX":
				options.NX = true
			case "XX":
				options.XX = true
			case "CH":
				options.CH = true
			default:
				goto points
			}
			i++
		}
	points:
		if options.NX && options.XX || len(args)-i < 3 || (len(args)-i)%3 != 0 {
			return errors.New("syntax error")
		}
		points := make([]engine.GeoPoint, 0, (len(args)-i)/3)
		for i < len(args) {
			lon, err := geoFloat(args[i])
			if err != nil {
				return err
			}
			lat, err := geoFloat(args[i+1])
			if err != nil {
				return err
			}
			points = append(points, engine.GeoPoint{Longitude: lon, Latitude: lat, Member: args[i+2]})
			i += 3
		}
		result, err := h.db.GeoAdd(key, points, options)
		if err != nil {
			return err
		}
		return w.WriteInt(result.Count)
	case "GEOPOS":
		positions, err := h.db.GeoPos(key, args[1:]...)
		if err != nil {
			return err
		}
		if err := w.WriteArrayHeader(len(positions)); err != nil {
			return err
		}
		for _, position := range positions {
			if position == nil {
				if err := w.WriteNil(); err != nil {
					return err
				}
				continue
			}
			if err := w.WriteArrayHeader(2); err != nil {
				return err
			}
			if err := w.WriteBlobString(geoNumber(position.Longitude)); err != nil {
				return err
			}
			if err := w.WriteBlobString(geoNumber(position.Latitude)); err != nil {
				return err
			}
		}
		return nil
	case "GEODIST":
		unit := 1.0
		if len(args) == 4 {
			var err error
			unit, err = geoUnit(args[3])
			if err != nil {
				return err
			}
		}
		distance, found, err := h.db.GeoDist(key, args[1], args[2])
		if err != nil {
			return err
		}
		if !found {
			return w.WriteNil()
		}
		return w.WriteBlobString(geoNumber(distance / unit))
	case "GEOHASH":
		hashes, err := h.db.GeoHash(key, args[1:]...)
		if err != nil {
			return err
		}
		if err := w.WriteArrayHeader(len(hashes)); err != nil {
			return err
		}
		for _, hash := range hashes {
			if hash == nil {
				err = w.WriteNil()
			} else {
				err = w.WriteBlobString([]byte(*hash))
			}
			if err != nil {
				return err
			}
		}
		return nil
	case "GEOSEARCH":
		options, unit, withDist, withHash, withCoord, err := parseGeoSearch(args[1:])
		if err != nil {
			return err
		}
		results, err := h.db.GeoSearch(key, options)
		if err != nil {
			return err
		}
		if err := w.WriteArrayHeader(len(results)); err != nil {
			return err
		}
		for _, item := range results {
			count := 0
			if withDist {
				count++
			}
			if withHash {
				count++
			}
			if withCoord {
				count++
			}
			if count == 0 {
				if err := w.WriteBlobString([]byte(item.Member)); err != nil {
					return err
				}
				continue
			}
			if err := w.WriteArrayHeader(count + 1); err != nil {
				return err
			}
			if err := w.WriteBlobString([]byte(item.Member)); err != nil {
				return err
			}
			if withDist {
				if err := w.WriteBlobString(geoNumber(item.Distance / unit)); err != nil {
					return err
				}
			}
			if withHash {
				if err := w.WriteInt64(int64(item.Hash)); err != nil {
					return err
				}
			}
			if withCoord {
				if err := w.WriteArrayHeader(2); err != nil {
					return err
				}
				if err := w.WriteBlobString(geoNumber(item.Longitude)); err != nil {
					return err
				}
				if err := w.WriteBlobString(geoNumber(item.Latitude)); err != nil {
					return err
				}
			}
		}
		return nil
	}
	return errors.New("unsupported geo command")
}

func parseGeoSearch(args []string) (engine.GeoSearchOptions, float64, bool, bool, bool, error) {
	var options engine.GeoSearchOptions
	unit := 1.0
	withDist, withHash, withCoord := false, false, false
	bad := func() (engine.GeoSearchOptions, float64, bool, bool, bool, error) {
		return options, unit, false, false, false, errors.New("syntax error")
	}
	i := 0
	if i >= len(args) {
		return bad()
	}
	switch strings.ToUpper(args[i]) {
	case "FROMMEMBER":
		if i+1 >= len(args) {
			return bad()
		}
		options.UseMember = true
		options.FromMember = args[i+1]
		i += 2
	case "FROMLONLAT":
		if i+2 >= len(args) {
			return bad()
		}
		var err error
		options.Longitude, err = geoFloat(args[i+1])
		if err != nil {
			return options, unit, false, false, false, err
		}
		options.Latitude, err = geoFloat(args[i+2])
		if err != nil {
			return options, unit, false, false, false, err
		}
		i += 3
	default:
		return bad()
	}
	if i >= len(args) {
		return bad()
	}
	switch strings.ToUpper(args[i]) {
	case "BYRADIUS":
		if i+2 >= len(args) {
			return bad()
		}
		radius, err := geoFloat(args[i+1])
		if err != nil {
			return options, unit, false, false, false, err
		}
		unit, err = geoUnit(args[i+2])
		if err != nil {
			return options, unit, false, false, false, err
		}
		options.Radius = radius * unit
		i += 3
	case "BYBOX":
		if i+3 >= len(args) {
			return bad()
		}
		width, err := geoFloat(args[i+1])
		if err != nil {
			return options, unit, false, false, false, err
		}
		height, err := geoFloat(args[i+2])
		if err != nil {
			return options, unit, false, false, false, err
		}
		unit, err = geoUnit(args[i+3])
		if err != nil {
			return options, unit, false, false, false, err
		}
		options.Box = true
		options.Width = width * unit
		options.Height = height * unit
		i += 4
	default:
		return bad()
	}
	seenOrder := false
	seenCount := false
	for i < len(args) {
		switch strings.ToUpper(args[i]) {
		case "ASC", "DESC":
			if seenOrder {
				return bad()
			}
			seenOrder = true
			options.Desc = strings.EqualFold(args[i], "DESC")
			i++
		case "COUNT":
			if seenCount || i+1 >= len(args) {
				return bad()
			}
			seenCount = true
			count, err := strconv.Atoi(args[i+1])
			if err != nil || count <= 0 {
				return bad()
			}
			options.Count = count
			i += 2
			if i < len(args) && strings.EqualFold(args[i], "ANY") {
				i++
			}
		case "WITHDIST":
			if withDist {
				return bad()
			}
			withDist = true
			i++
		case "WITHHASH":
			if withHash {
				return bad()
			}
			withHash = true
			i++
		case "WITHCOORD":
			if withCoord {
				return bad()
			}
			withCoord = true
			i++
		default:
			return bad()
		}
	}
	if options.Radius < 0 || options.Width < 0 || options.Height < 0 {
		return options, unit, false, false, false, engine.ErrInvalidGeo
	}
	return options, unit, withDist, withHash, withCoord, nil
}
