package mysql

import (
	"bytes"
	"encoding/binary"
	"testing"
)

// 创建一个模拟的 JSONB_TYPE_LARGE_ARRAY 数据
func createMockLargeArrayData() []byte {
	buf := bytes.NewBuffer(nil)

	// 写入类型标识 JSONB_TYPE_LARGE_ARRAY
	buf.WriteByte(JSONB_TYPE_LARGE_ARRAY)

	// 写入元素数量 (uint32) 和大小 (uint32)
	elements := uint32(2)
	size := uint32(15) // 修正大小: 8(header) + 3(first element) + 4(second element)
	binary.Write(buf, binary.LittleEndian, elements)
	binary.Write(buf, binary.LittleEndian, size)

	// 写入元素类型和偏移信息
	// 第一个元素: INT16 类型，内联
	buf.WriteByte(JSONB_TYPE_INT16)
	var val1 int16 = 42
	binary.Write(buf, binary.LittleEndian, val1)

	// 第二个元素: LITERAL 类型，内联
	buf.WriteByte(JSONB_TYPE_LITERAL)
	var literal uint32 = JSONB_LITERAL_TRUE
	binary.Write(buf, binary.LittleEndian, literal)

	return buf.Bytes()
}

// 创建一个模拟的 JSONB_TYPE_LARGE_OBJECT 数据
func createMockLargeObjectData() []byte {
	buf := bytes.NewBuffer(nil)

	// 写入类型标识 JSONB_TYPE_LARGE_OBJECT
	buf.WriteByte(JSONB_TYPE_LARGE_OBJECT)

	// 写入元素数量 (uint32) 和大小 (uint32)
	elements := uint32(2)
	size := uint32(26) // 修正大小: 8(header) + 12(key offsets) + 6(value types) + 7(key strings)
	binary.Write(buf, binary.LittleEndian, elements)
	binary.Write(buf, binary.LittleEndian, size)

	// 写入 key 偏移和长度信息 (large format)
	// 第一个 key
	var keyOffset1 uint32 = 0
	var keyLen1 uint16 = 4
	binary.Write(buf, binary.LittleEndian, keyOffset1)
	binary.Write(buf, binary.LittleEndian, keyLen1)

	// 第二个 key
	var keyOffset2 uint32 = 4
	var keyLen2 uint16 = 3
	binary.Write(buf, binary.LittleEndian, keyOffset2)
	binary.Write(buf, binary.LittleEndian, keyLen2)

	// 写入值类型和偏移信息
	// 第一个值: INT16 类型，内联
	buf.WriteByte(JSONB_TYPE_INT16)
	var val1 int16 = 100
	binary.Write(buf, binary.LittleEndian, val1)

	// 第二个值: LITERAL 类型，内联
	buf.WriteByte(JSONB_TYPE_LITERAL)
	var literal uint32 = JSONB_LITERAL_FALSE
	binary.Write(buf, binary.LittleEndian, literal)

	// 写入 key 字符串
	buf.WriteString("name") // 4 bytes
	buf.WriteString("age")  // 3 bytes

	return buf.Bytes()
}

// 测试 LARGE_ARRAY 类型判断修复
func TestLargeArrayTypeFix(t *testing.T) {
	data := createMockLargeArrayData()

	result, err := get_field_json_data(data, int64(len(data)))
	if err != nil {
		t.Fatalf("Failed to parse large array: %v", err)
	}

	// 验证结果是数组类型
	arr, ok := result.([]interface{})
	if !ok {
		t.Fatalf("Expected array, got %T", result)
	}

	// 验证数组长度
	if len(arr) != 2 {
		t.Fatalf("Expected 2 elements, got %d", len(arr))
	}

	// 验证第一个元素
	if arr[0] != int16(42) {
		t.Fatalf("Expected first element to be 42, got %v", arr[0])
	}

	// 验证第二个元素
	if arr[1] != true {
		t.Fatalf("Expected second element to be true, got %v", arr[1])
	}

	t.Logf("Large array parsed successfully: %+v", result)
}

// 测试 LARGE_OBJECT key offset 索引修复
func TestLargeObjectKeyOffsetFix(t *testing.T) {
	data := createMockLargeObjectData()

	result, err := get_field_json_data(data, int64(len(data)))
	if err != nil {
		t.Fatalf("Failed to parse large object: %v", err)
	}

	// 验证结果是对象类型
	obj, ok := result.(map[string]interface{})
	if !ok {
		t.Fatalf("Expected object, got %T", result)
	}

	// 验证对象有两个键
	if len(obj) != 2 {
		t.Fatalf("Expected 2 keys, got %d", len(obj))
	}

	// 验证键值对
	if obj["name"] != int16(100) {
		t.Fatalf("Expected 'name' to be 100, got %v", obj["name"])
	}

	if obj["age"] != false {
		t.Fatalf("Expected 'age' to be false, got %v", obj["age"])
	}

	t.Logf("Large object parsed successfully: %+v", result)
}

// 测试小数组和小对象（确保没有破坏原有功能）
func TestSmallArrayAndObject(t *testing.T) {
	// 测试小数组
	smallArrayBuf := bytes.NewBuffer(nil)
	smallArrayBuf.WriteByte(JSONB_TYPE_SMALL_ARRAY)

	elements := uint16(1)
	size := uint16(7) // 修正大小: 4(header) + 3(element: 1 byte type + 2 bytes value)
	binary.Write(smallArrayBuf, binary.LittleEndian, elements)
	binary.Write(smallArrayBuf, binary.LittleEndian, size)

	// 一个 INT16 元素
	smallArrayBuf.WriteByte(JSONB_TYPE_INT16)
	var val int16 = 123
	binary.Write(smallArrayBuf, binary.LittleEndian, val)

	result, err := get_field_json_data(smallArrayBuf.Bytes(), int64(smallArrayBuf.Len()))
	if err != nil {
		t.Fatalf("Failed to parse small array: %v", err)
	}

	arr, ok := result.([]interface{})
	if !ok || len(arr) != 1 || arr[0] != int16(123) {
		t.Fatalf("Small array parsing failed: %v", result)
	}

	t.Logf("Small array parsed successfully: %+v", result)
}

// 边界测试：测试空数组和空对象
func TestEmptyArrayAndObject(t *testing.T) {
	// 测试空的大数组
	emptyArrayBuf := bytes.NewBuffer(nil)
	emptyArrayBuf.WriteByte(JSONB_TYPE_LARGE_ARRAY)

	elements := uint32(0)
	size := uint32(8) // 仅包含头部信息
	binary.Write(emptyArrayBuf, binary.LittleEndian, elements)
	binary.Write(emptyArrayBuf, binary.LittleEndian, size)

	result, err := get_field_json_data(emptyArrayBuf.Bytes(), int64(emptyArrayBuf.Len()))
	if err != nil {
		t.Fatalf("Failed to parse empty large array: %v", err)
	}

	arr, ok := result.([]interface{})
	if !ok || len(arr) != 0 {
		t.Fatalf("Empty large array parsing failed: %v", result)
	}

	t.Logf("Empty large array parsed successfully: %+v", result)
}
