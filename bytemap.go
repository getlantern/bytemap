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
	return Build(func(cb func(string, interface{})) {
		for key, value := range m {
			cb(key, value)
		}
	}, func(key string) interface{} {
		return m[key]
	}, false)
}

// NewFloat creates a new ByteMap from the given map
func NewFloat(m map[string]float64) ByteMap {
	return Build(func(cb func(string, interface{})) {
		for key, value := range m {
			cb(key, value)
		}
	}, func(key string) interface{} {
		return m[key]
	}, false)
}

// FromSortedKeysAndValues constructs a ByteMap from sorted keys and values.
func FromSortedKeysAndValues(keys []string, values []interface{}) ByteMap {
	return Build(func(cb func(string, interface{})) {
		for i, key := range keys {
			cb(key, values[i])
		}
	}, nil, true)
}

// FromSortedKeysAndFloats constructs a ByteMap from sorted keys and float values.
func FromSortedKeysAndFloats(keys []string, values []float64) ByteMap {
	return Build(func(cb func(string, interface{})) {
		for i, key := range keys {
			cb(key, values[i])
		}
	}, nil, true)
}

// Build builds a new ByteMap using a function that iterates overall included
// key/value paris and another function that returns the value for a given key/
// index. If iteratesSorted is true, then the iterate order of iterate is
// considered to be in lexicographically sorted order over the keys and is
// stable over multiple invocations, and valueFor is not needed.
func Build(iterate func(func(string, interface{})), valueFor func(string) interface{}, iteratesSorted bool) ByteMap {
	keysLen := 0
	valuesLen := 0

	recordKey := func(key string, value interface{}) {
		valLen := encodedLength(value)
		keysLen += len(key) + SizeKeyLen + SizeValueType
		if valLen > 0 {
			keysLen += SizeValueOffset
		}
		valuesLen += valLen
	}

	var finalIterate func(func(string, interface{}))

	if iteratesSorted {
		iterate(func(key string, value interface{}) {
			recordKey(key, value)
		})
		finalIterate = iterate
	} else {
		sortedKeys := make([]string, 0, 10)
		iterate(func(key string, value interface{}) {
			sortedKeys = append(sortedKeys, key)
			recordKey(key, value)
		})
		sort.Strings(sortedKeys)

		finalIterate = func(cb func(string, interface{})) {
			for _, key := range sortedKeys {
				cb(key, valueFor(key))
			}
		}
	}

	startOfValues := keysLen
	bm := make(ByteMap, startOfValues+valuesLen)
	keyOffset := 0
	valueOffset := startOfValues
	finalIterate(func(key string, value interface{}) {
		keyLen := len(key)
		enc.PutUint16(bm[keyOffset:], uint16(keyLen))
		copy(bm[keyOffset+SizeKeyLen:], key)
		keyOffset += SizeKeyLen + keyLen
		t, n := encodeValue(bm[valueOffset:], value)
		bm[keyOffset] = t
		keyOffset += SizeValueType
		if t != TypeNil {
			enc.PutUint32(bm[keyOffset:], uint32(valueOffset))
			keyOffset += SizeValueOffset
			valueOffset += n
		}
	})

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
	bm.IterateValues(func(key string, value interface{}) bool {
		result[key] = value
		return true
	})
	return result
}

// IterateValues iterates over the key/value pairs in this ByteMap and calls the
// given callback with each. If the callback returns false, iteration stops even
// if there remain unread values.
func (bm ByteMap) IterateValues(cb func(key string, value interface{}) bool) {
	bm.Iterate(true, false, func(key string, value interface{}, valueBytes []byte) bool {
		return cb(key, value)
	})
}

// IterateValueBytes iterates over the key/value bytes pairs in this ByteMap and
// calls the given callback with each. If the callback returns false, iteration
// stops even if there remain unread values.
func (bm ByteMap) IterateValueBytes(cb func(key string, valueBytes []byte) bool) {
	bm.Iterate(false, true, func(key string, value interface{}, valueBytes []byte) bool {
		return cb(key, valueBytes)
	})
}

// Iterate iterates over the key/value pairs in this ByteMap and calls the given
// callback with each. If the callback returns false, iteration stops even if
// there remain unread values. includeValue and includeBytes determine whether
// to include the value, the bytes or both in the callback.
func (bm ByteMap) Iterate(includeValue bool, includeBytes bool, cb func(key string, value interface{}, valueBytes []byte) bool) {
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
		var bytes []byte
		if t != TypeNil {
			valueOffset := int(enc.Uint32(bm[keyOffset:]))
			if firstValueOffset == 0 {
				firstValueOffset = valueOffset
			}
			slice := bm[valueOffset:]
			if includeValue {
				value = decodeValue(slice, t)
			}
			if includeBytes {
				bytes = valueBytes(slice, t)
			}
			keyOffset += SizeValueOffset
		}
		if !cb(key, value, bytes) {
			// Stop iterating
			return
		}
		if firstValueOffset > 0 && keyOffset >= firstValueOffset {
			break
		}
	}
}

// Slice creates a new ByteMap that contains only the specified keys from the
// original.
func (bm ByteMap) Slice(keys ...string) ByteMap {
	result, _ := bm.doSplit(false, keys)
	return result
}

// Split returns two byte maps, the first containing all of the specified keys
// and the second containing all of the other keys.
func (bm ByteMap) Split(keys ...string) (ByteMap, ByteMap) {
	return bm.doSplit(true, keys)
}

func (bm ByteMap) doSplit(includeOmitted bool, keys []string) (ByteMap, ByteMap) {
	sort.Strings(keys)
	keyBytes := make([][]byte, 0, len(keys))
	for _, key := range keys {
		keyBytes = append(keyBytes, []byte(key))
	}
	matchedKeys := make([][]byte, 0, len(keys))
	matchedValueOffsets := make([]int, 0, len(keys))
	matchedValues := make([][]byte, 0, len(keys))
	matchedKeysLen := 0
	matchedValuesLen := 0
	var omittedKeys [][]byte
	var omittedValueOffsets []int
	var omittedValues [][]byte
	omittedKeysLen := 0
	omittedValuesLen := 0
	if includeOmitted {
		omittedKeys = make([][]byte, 0, 10)
		omittedValueOffsets = make([]int, 0, 10)
		omittedValues = make([][]byte, 0, 10)
	}
	keyOffset := 0
	firstValueOffset := 0

	advance := func(candidate []byte) bool {
		if keyBytes == nil {
			return false
		}
		var keyComparison int
		for {
			key := keyBytes[0]
			keyComparison = bytes.Compare(candidate, key)
			for {
				if keyComparison <= 0 {
					break
				}
				// Candidate is past key, consume keys
				if len(keyBytes) > 1 {
					keyBytes = keyBytes[1:]
				} else {
					keyBytes = nil
					break
				}
				key = keyBytes[0]
				keyComparison = bytes.Compare(candidate, key)
			}
			return keyComparison == 0
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
		matched := advance(candidate)
		keyOffset += keyLen
		t := bm[keyOffset]
		keyOffset += SizeValueType
		if t != TypeNil {
			valueOffset := int(enc.Uint32(bm[keyOffset:]))
			if firstValueOffset == 0 {
				firstValueOffset = valueOffset
			}
			valueLen := bm.lengthOf(valueOffset, t)
			value := bm[valueOffset : valueOffset+valueLen]

			if matched {
				matchedKeys = append(matchedKeys, bm[keyStart:keyOffset])
				matchedValueOffsets = append(matchedValueOffsets, matchedValuesLen)
				matchedValues = append(matchedValues, value)
				matchedKeysLen += keyOffset + SizeValueOffset - keyStart
				matchedValuesLen += valueLen
			} else if includeOmitted {
				omittedKeys = append(omittedKeys, bm[keyStart:keyOffset])
				omittedValueOffsets = append(omittedValueOffsets, omittedValuesLen)
				omittedValues = append(omittedValues, value)
				omittedKeysLen += keyOffset + SizeValueOffset - keyStart
				omittedValuesLen += valueLen
			}

			keyOffset += SizeValueOffset
		}

		if keyOffset >= firstValueOffset {
			break
		}

		if !includeOmitted && len(keyBytes) == 0 {
			break
		}
	}

	included := buildFromSliced(matchedKeysLen, matchedValuesLen, matchedKeys, matchedValueOffsets, matchedValues)
	var omitted ByteMap
	if includeOmitted {
		omitted = buildFromSliced(omittedKeysLen, omittedValuesLen, omittedKeys, omittedValueOffsets, omittedValues)
	}
	return included, omitted
}

func buildFromSliced(keysLen int, valuesLen int, keys [][]byte, valueOffsets []int, values [][]byte) ByteMap {
	out := make(ByteMap, keysLen+valuesLen)
	offset := 0
	for i, kb := range keys {
		valueOffset := valueOffsets[i]
		copy(out[offset:], kb)
		offset += len(kb)
		if valueOffset >= 0 {
			enc.PutUint32(out[offset:], uint32(valueOffset+keysLen))
			offset += SizeValueOffset
		}
	}
	for _, vb := range values {
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
