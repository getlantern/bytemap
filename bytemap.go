// Package bytemap provides a map[string]interface{} encoded as a byte slice.
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
	TypeInt
	TypeFloat32
	TypeFloat64
	TypeString
	TypeTime
	TypeUInt
)

const (
	SizeKeyLen      = 2
	SizeValueType   = 1
	SizeValueOffset = 4
)

var (
	enc = binary.LittleEndian
)

// ByteMap is an immutable map[string]interface{} backed by a byte array.
type ByteMap []byte

// New creates a new ByteMap from the given map
func New(m map[string]interface{}) ByteMap {
	sortedKeys := make([]string, 0, len(m))
	keysLen := 0
	valuesLen := 0
	for key, value := range m {
		sortedKeys = append(sortedKeys, key)
		valLen := encodedLength(value)
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

// FromSortedKeysAndValues constructs a ByteMap from sorted keys and values.
func FromSortedKeysAndValues(keys []string, values []interface{}) ByteMap {
	return doFromSortedKeysAndValues(keys, interfaceValues(values))
}

// FromSortedKeysAndFloats constructs a ByteMap from sorted keys and float values.
func FromSortedKeysAndFloats(keys []string, values []float64) ByteMap {
	return doFromSortedKeysAndValues(keys, floatValues(values))
}

func doFromSortedKeysAndValues(keys []string, vals valuesIF) ByteMap {
	keysLen := 0
	for _, key := range keys {
		keysLen += len(key) + SizeKeyLen + SizeValueType
	}
	valuesLen := 0
	for i := 0; i < len(keys); i++ {
		value := vals.get(i)
		valLen := encodedLength(value)
		valuesLen += valLen
		if valLen > 0 {
			keysLen += SizeValueOffset
		}
	}

	startOfValues := keysLen
	bm := make(ByteMap, startOfValues+valuesLen)
	keyOffset := 0
	valueOffset := startOfValues
	for i, key := range keys {
		keyLen := len(key)
		enc.PutUint16(bm[keyOffset:], uint16(keyLen))
		copy(bm[keyOffset+SizeKeyLen:], key)
		keyOffset += SizeKeyLen + keyLen
		t, n := encodeValue(bm[valueOffset:], vals.get(i))
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

// NewFLoat creates a new ByteMap from the given map
func NewFloat(m map[string]float64) ByteMap {
	// TODO: this code is duplicated with the above, need to get DRY
	sortedKeys := make([]string, 0, len(m))
	keysLen := 0
	valuesLen := 0
	for key, value := range m {
		sortedKeys = append(sortedKeys, key)
		valLen := encodedLength(value)
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

// Get gets the value for the given key, or nil if the key is not found.
func (bm ByteMap) Get(key string) interface{} {
	keyBytes := []byte(key)
	keyOffset := 0
	firstValueOffset := 0
	for {
		if keyOffset >= len(bm) {
			break
		}
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
	}
	return nil
}

// GetBytes gets the bytes slice for the given key, or nil if the key is not
// found.
func (bm ByteMap) GetBytes(key string) []byte {
	keyBytes := []byte(key)
	keyOffset := 0
	firstValueOffset := 0
	for {
		if keyOffset >= len(bm) {
			break
		}
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
				return valueBytes(bm[valueOffset:], t)
			}
			keyOffset += SizeValueOffset
		}
		if firstValueOffset > 0 && keyOffset >= firstValueOffset {
			break
		}
	}
	return nil
}

// AsMap returns a map representation of this ByteMap.
func (bm ByteMap) AsMap() map[string]interface{} {
	result := make(map[string]interface{}, 10)
	bm.Iterate(func(key string, value interface{}) bool {
		result[key] = value
		return true
	})
	return result
}

func (bm ByteMap) Iterate(cb func(key string, value interface{}) bool) {
	if len(bm) == 0 {
		return
	}

	keyOffset := 0
	firstValueOffset := 0
	for {
		if keyOffset >= len(bm) {
			break
		}
		keyLen := int(enc.Uint16(bm[keyOffset:]))
		keyOffset += SizeKeyLen
		key := string(bm[keyOffset : keyOffset+keyLen])
		keyOffset += keyLen
		t := bm[keyOffset]
		keyOffset += SizeValueType
		var value interface{}
		if t == TypeNil {
			value = nil
		} else {
			valueOffset := int(enc.Uint32(bm[keyOffset:]))
			if firstValueOffset == 0 {
				firstValueOffset = valueOffset
			}
			value = decodeValue(bm[valueOffset:], t)
			keyOffset += SizeValueOffset
		}
		if !cb(key, value) {
			// Stop iterating
			return
		}
		if firstValueOffset > 0 && keyOffset >= firstValueOffset {
			break
		}
	}
}

// Slice creates a new ByteMap that contains only the specified keys from the
// original. Any keys that were not found in the original will appear in the
// result with a nil value.
func (bm ByteMap) Slice(keys ...string) ByteMap {
	sort.Strings(keys)
	keyBytes := make([][]byte, 0, len(keys))
	for _, key := range keys {
		keyBytes = append(keyBytes, []byte(key))
	}
	matchedKeys := make([][]byte, 0, len(keys))
	matchedValues := make([][]byte, 0, len(keys))
	valueOffsets := make([]int, 0, len(keys))
	keysLen := 0
	valuesLen := 0
	keyOffset := 0
	firstValueOffset := 0
	matched := false

	advance := func(candidate []byte) {
		for {
			key := keyBytes[0]
			keyComparison := bytes.Compare(candidate, key)
			if keyComparison > 0 {
				// Key not represented, skip it
				keyLen := len(key)
				b := make([]byte, SizeKeyLen+keyLen+SizeValueType)
				enc.PutUint16(b, uint16(keyLen))
				copy(b[SizeKeyLen:], key)
				keysLen += len(b)
				matchedKeys = append(matchedKeys, b)
				valueOffsets = append(valueOffsets, -1)
				if len(keyBytes) > 1 {
					keyBytes = keyBytes[1:]
					continue
				} else {
					keyBytes = nil
					break
				}
			}
			matched = keyComparison == 0
			if matched {
				if len(keyBytes) > 1 {
					keyBytes = keyBytes[1:]
				} else {
					keyBytes = nil
				}
			}
			break
		}
	}

	for {
		if keyOffset >= len(bm) {
			break
		}
		keyStart := keyOffset
		keyLen := int(enc.Uint16(bm[keyOffset:]))
		keyOffset += SizeKeyLen
		candidate := bm[keyOffset : keyOffset+keyLen]
		advance(candidate)
		keyOffset += keyLen
		t := bm[keyOffset]
		keyOffset += SizeValueType
		if matched {
			matchedKeys = append(matchedKeys, bm[keyStart:keyOffset])
			if t == TypeNil {
				valueOffsets = append(valueOffsets, -1)
			} else {
				valueOffset := int(enc.Uint32(bm[keyOffset:]))
				valueLen := bm.lengthOf(valueOffset, t)
				matchedValues = append(matchedValues, bm[valueOffset:valueOffset+valueLen])
				valueOffsets = append(valueOffsets, valuesLen)
				keysLen += SizeValueOffset
				valuesLen += valueLen
			}
			keysLen += keyOffset - keyStart
		}
		if t != TypeNil {
			keyOffset += SizeValueOffset
		}
		if keyBytes == nil {
			break
		}
		if t != TypeNil {
			if firstValueOffset == 0 {
				firstValueOffset = int(enc.Uint32(bm[keyOffset:]))
			}
		}
		if keyOffset >= firstValueOffset {
			break
		}
		if len(keyBytes) == 0 {
			break
		}
	}

	out := make(ByteMap, keysLen+valuesLen)
	offset := 0
	for i, kb := range matchedKeys {
		valueOffset := valueOffsets[i]
		copy(out[offset:], kb)
		offset += len(kb)
		if valueOffset >= 0 {
			enc.PutUint32(out[offset:], uint32(valueOffset+keysLen))
			offset += SizeValueOffset
		}
	}
	for _, vb := range matchedValues {
		copy(out[offset:], vb)
		offset += len(vb)
	}

	return out
}

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
	case uint:
		enc.PutUint64(slice, uint64(v))
		return TypeUInt, 8
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
	case int:
		enc.PutUint64(slice, uint64(v))
		return TypeInt, 8
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
	case TypeUInt:
		return uint(enc.Uint64(slice))
	case TypeInt8:
		return int8(slice[0])
	case TypeInt16:
		return int16(enc.Uint16(slice))
	case TypeInt32:
		return int32(enc.Uint32(slice))
	case TypeInt64:
		return int64(enc.Uint64(slice))
	case TypeInt:
		return int(enc.Uint64(slice))
	case TypeFloat32:
		return math.Float32frombits(enc.Uint32(slice))
	case TypeFloat64:
		return math.Float64frombits(enc.Uint64(slice))
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

func valueBytes(slice []byte, t byte) []byte {
	switch t {
	case TypeBool, TypeByte, TypeInt8:
		return slice[:1]
	case TypeUInt16, TypeInt16:
		return slice[:2]
	case TypeUInt32, TypeInt32, TypeFloat32:
		return slice[:4]
	case TypeUInt64, TypeUInt, TypeInt64, TypeInt, TypeFloat64:
		return slice[:8]
	case TypeString:
		l := int(enc.Uint16(slice))
		return slice[0 : l+2]
	case TypeTime:
		return slice[:8]
	}
	return nil
}

func encodedLength(value interface{}) int {
	switch v := value.(type) {
	case bool, byte, int8:
		return 1
	case uint16, int16:
		return 2
	case uint32, int32, float32:
		return 4
	case uint64, int64, uint, int, float64, time.Time:
		return 8
	case string:
		return len(v) + 2
	}
	return 0
}

func (bm ByteMap) lengthOf(valueOffset int, t byte) int {
	switch t {
	case TypeBool, TypeByte, TypeInt8:
		return 1
	case TypeUInt16, TypeInt16:
		return 2
	case TypeUInt32, TypeInt32, TypeFloat32:
		return 4
	case TypeUInt64, TypeInt64, TypeUInt, TypeInt, TypeFloat64, TypeTime:
		return 8
	case TypeString:
		return int(enc.Uint16(bm[valueOffset:])) + 2
	}
	return 0
}
