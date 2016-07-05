package bytemap

import (
	"bytes"
	"encoding/binary"
	"math"
	"sort"
	"time"
)

const (
	TypeNil = iota
	TypeBool
	TypeByte
	TypeUInt16
	TypeUInt32
	TypeUInt64
	TypeInt8
	TypeInt16
	TypeInt32
	TypeInt64
	TypeFloat32
	TypeFloat64
	TypeString
	TypeTime
)

const (
	SizeKeyLen      = 2
	SizeValueType   = 1
	SizeValueOffset = 4
)

var (
	enc = binary.BigEndian
)

type ByteMap []byte

func New(m map[string]interface{}) ByteMap {
	sortedKeys := make([]string, 0, len(m))
	keysLen := 0
	valuesLen := 0
	for key, value := range m {
		sortedKeys = append(sortedKeys, key)
		valLen := lengthOf(value)
		keysLen += len(key) + SizeKeyLen + SizeValueType
		if valLen > 0 {
			keysLen += SizeValueOffset
		}
		valuesLen += valLen
	}
	sort.Strings(sortedKeys)

	startOfValues := keysLen
	bm := make(ByteMap, startOfValues+valuesLen)
	keyOffset := 0
	valueOffset := startOfValues
	for _, key := range sortedKeys {
		keyLen := len(key)
		enc.PutUint16(bm[keyOffset:], uint16(keyLen))
		copy(bm[keyOffset+SizeKeyLen:], key)
		keyOffset += SizeKeyLen + keyLen
		t, n := encodeValue(bm[valueOffset:], m[key])
		bm[keyOffset] = t
		keyOffset += SizeValueType
		if t != TypeNil {
			enc.PutUint32(bm[keyOffset:], uint32(valueOffset))
			keyOffset += SizeValueOffset
			valueOffset += n
		}
	}

	return bm
}

func (bm ByteMap) Get(key string) interface{} {
	keyBytes := []byte(key)
	keyOffset := 0
	firstValueOffset := 0
	for {
		keyLen := int(enc.Uint16(bm[keyOffset:]))
		keyOffset += SizeKeyLen
		keysMatch := bytes.Equal(bm[keyOffset:keyOffset+keyLen], keyBytes)
		keyOffset += keyLen
		t := bm[keyOffset]
		keyOffset += SizeValueType
		if t == TypeNil {
			if keysMatch {
				return nil
			}
		} else {
			valueOffset := int(enc.Uint32(bm[keyOffset:]))
			if firstValueOffset == 0 {
				firstValueOffset = valueOffset
			}
			if keysMatch {
				return decodeValue(bm[valueOffset:], t)
			}
			keyOffset += SizeValueOffset
		}
		if firstValueOffset > 0 && keyOffset >= firstValueOffset {
			break
		}
		if keyOffset >= len(bm) {
			break
		}
	}
	return nil
}

// func (bm ByteMap) Slice(keys ...string) ByteMap {
// 	keyBytes := make([][]byte, len(keys))
// 	for _, key := range keys {
// 		keyBytes = append(keyBytes, []byte(key))
// 	}
// 	matchedKeys := make([][]byte, 0, len(keys))
// 	matchedValues := make([][]byte, 0, len(keys))
// 	keysLen := 0
// 	valuesLen := 0
// 	lastKeyMatched := false
// 	lastValueOffset := 0
// 	keyOffset := 0
// 	firstValueOffset := 0
//   more := true
//   matched := false
//
//   advanceKeyMatch := func(candidate []byte) {
//   	for {
//   		keyComparison := bytes.Compare(candidate, keyBytes[0])
//       more = len(keyBytes) > 1
//       if keyComparison > 0 {
//         // Key not represented, skip it
//         keyLen = len(keyBytes[0])
//     		b := make([]byte, SizeKeyLen + SizeValueOffset, keyLen)
//         enc.PutUint16(b, keyLen)
//         copy(b[SizeKeyLen:], keyBytes[0])
//         enc.PutUint32(b[SizeKeyLen+keyLen:], )
//       }
//       matched = keyComparison == 0
//       if keyCompari
//       if keyComaprison ==
//   		if keyComparison >= 0 {
//   			matched := keyComparison == 0
//   			if more {
//   				return matched, keyBytes[1:]
//   			}
//   			return matched, nil
//   		}
//   		if !more {
//   			return false, nil
//   		}
//   		keyBytes = keyBytes[1:]
//   	}
//   }
// 	for {
// 		keyStart := keyOffset
// 		keyLen := int(enc.Uint16(bm[keyOffset:]))
// 		keyOffset += SizeKeyLen
// 		candidate := bm[keyOffset : keyOffset+keyLen]
// 		keysMatch := false
// 		keysMatch, keyBytes = advanceKeyMatch(candidate, keyBytes)
// 		keyOffset += keyLen + SizeValueType
// 		valueOffset := int(enc.Uint32(bm[keyOffset:]))
// 		if firstValueOffset == 0 {
// 			firstValueOffset = valueOffset
// 		}
// 		if lastKeyMatched {
// 			matchedValues = append(matchedValues, bm[lastValueOffset:valueOffset])
// 			valuesLen += valueOffset - lastValueOffset
// 		}
// 		lastValueOffset = valueOffset
// 		lastKeyMatched = keysMatch
// 		if keysMatch {
// 			matchedKeys = append(matchedKeys, bm[keyStart:keyOffset])
// 			keysLen += keyOffset - keyStart
// 		}
// 		if keyBytes == nil {
// 			break
// 		}
// 		keyOffset += SizeValueOffset
// 		if keyOffset >= firstValueOffset {
// 			break
// 		}
// 	}
//
// 	if lastKeyMatched {
// 		matchedValues = append(matchedValues, bm[lastValueOffset:])
// 		valuesLen += len(bm) - lastValueOffset
// 	}
//
// 	out := make(ByteMap, 0, keysLen+valuesLen)
// 	for _, kb := range matchedKeys {
// 		out = append(out, kb...)
// 	}
// 	for _, vb := range matchedValues {
// 		out = append(out, vb...)
// 	}
//
// 	return out
// }

func encodeValue(slice []byte, value interface{}) (byte, int) {
	switch v := value.(type) {
	case bool:
		if v {
			slice[0] = 1
		} else {
			slice[0] = 0
		}
		return TypeBool, 1
	case byte:
		slice[0] = v
		return TypeByte, 1
	case uint16:
		enc.PutUint16(slice, v)
		return TypeUInt16, 2
	case uint32:
		enc.PutUint32(slice, v)
		return TypeUInt32, 4
	case uint64:
		enc.PutUint64(slice, v)
		return TypeUInt64, 8
	case int8:
		slice[0] = byte(v)
		return TypeInt8, 1
	case int16:
		enc.PutUint16(slice, uint16(v))
		return TypeInt16, 2
	case int32:
		enc.PutUint32(slice, uint32(v))
		return TypeInt32, 4
	case int64:
		enc.PutUint64(slice, uint64(v))
		return TypeInt64, 8
	case float32:
		enc.PutUint32(slice, math.Float32bits(v))
		return TypeFloat32, 4
	case float64:
		enc.PutUint64(slice, math.Float64bits(v))
		return TypeFloat64, 8
	case string:
		enc.PutUint16(slice, uint16(len(v)))
		copy(slice[2:], v)
		return TypeString, len(v) + 2
	case time.Time:
		enc.PutUint64(slice, uint64(v.UnixNano()))
		return TypeTime, 8
	}
	return TypeNil, 0
}

func decodeValue(slice []byte, t byte) interface{} {
	switch t {
	case TypeBool:
		return slice[0] == 1
	case TypeByte:
		return slice[0]
	case TypeUInt16:
		return enc.Uint16(slice)
	case TypeUInt32:
		return enc.Uint32(slice)
	case TypeUInt64:
		return enc.Uint64(slice)
	case TypeInt8:
		return int8(slice[0])
	case TypeInt16:
		return int16(enc.Uint16(slice))
	case TypeInt32:
		return int32(enc.Uint32(slice))
	case TypeInt64:
		return int64(enc.Uint64(slice))
	case TypeFloat32:
		return math.Float32frombits(enc.Uint32(slice))
	case TypeFloat64:
		return math.Float64frombits((enc.Uint64(slice)))
	case TypeString:
		l := int(enc.Uint16(slice))
		return string(slice[2 : l+2])
	case TypeTime:
		nanos := int64(enc.Uint64(slice))
		second := int64(time.Second)
		return time.Unix(nanos/second, nanos%second)
	}
	return nil
}

func lengthOf(value interface{}) int {
	switch v := value.(type) {
	case bool, byte, int8:
		return 1
	case uint16, int16, uint32, int32, float32:
		return 4
	case uint64, int64, float64, time.Time:
		return 8
	case string:
		return len(v) + 2
	}
	return 0
}
