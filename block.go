package smallfiles

import (
	"encoding/binary"
	"errors"
)

/**
	先假定block的容量大小就是1mb，不允许某个smallData的添加超过该限制
*/

const (
	BlockCapacity = 1024 * 1024
	)
type SmallData interface { // 标识存储的最下数据单元，如存取的一条文章信息
	Size() uint32
	Serialize() []byte
}

type SmallDataFormat func(bytes []byte) SmallData

type Block struct {
	Capacity uint32 	  // 该block的容量 如 1024 * 1024 (1mb)
	Length uint32         // 该block的byte字节长度, 即最后字节所在block中的offset
	DataSize uint16       // 该block中存放的smalldata的个数
	Offsets []uint32      // smalldata 对应的offset 数组
	Datas []SmallData
}

func GenerateBlock(datas []SmallData) *Block {
	length := uint32(10 + len(datas) * 4)
	offsets := []uint32{length}

	for i, d := range datas {
		length = length + d.Size()
		if i < len(datas) - 1 {
			offsets = append(offsets, length)
		}
	}

	b := &Block{
		Length:   length,
		DataSize: uint16(len(datas)),
		Offsets:  offsets,
		Datas:    datas,
	}

	return b
}

func GenerateBlockFromBytes(bytes []byte, sdf SmallDataFormat) *Block {
	// block 在磁盘中的数据格式：capacity[4byte] + length[4byte] + datasize[2byte] + offsets[n * 4byte] + datas[xxxbyte]
	cap := binary.BigEndian.Uint32(bytes[0:4])
	length := binary.BigEndian.Uint32(bytes[4:8])
	ds := binary.BigEndian.Uint16(bytes[8:10])

	var offsets []uint32
	var datas []SmallData
	for i:= uint16(0); i < ds; i ++ {
		o := binary.BigEndian.Uint32(bytes[10 + i*4 : 14 + i*4])
		offsets = append(offsets, o)
	}
	if len(offsets) < 1 {
		return nil
	}

	offset := offsets[0]
	for i, _ := range offsets {
		var os uint32
		if i == len(offsets) -1 {
			os = length
		} else {
			os = offsets[i + 1]
		}
		datas = append(datas, sdf(bytes[offset: os]))
		offset = os
	}

	b := &Block{
		Capacity: cap,
		Length:   length,
		Offsets:  offsets,
		DataSize: ds,
		Datas:    datas,
	}
	return b
}

func (b *Block) Bytes() []byte {
	out := make([]byte, BlockCapacity)
	binary.BigEndian.PutUint32(out[0:4], b.Capacity) // 先写入capacity

	binary.BigEndian.PutUint32(out[4:8], b.Length)  // 写入length

	binary.BigEndian.PutUint16(out[8:10], b.DataSize) // 写入datasize

	var offsets []uint32
	var offset uint32 = 10 + uint32(b.DataSize * 4)
	for i:=uint16(0); i < b.DataSize; i ++ {
		offsets = append(offsets, offset)
		binary.BigEndian.PutUint32(out[10+i*4: 14+i*4], offset)
		ds := b.Datas[i].Serialize()
		copy(out[offset: int(offset) + len(ds)], ds)
		offset = offset + uint32(len(ds))
	}

	return out
}

func (b *Block) AddData(s SmallData) error{
	offset := b.Length
	length := b.Length + s.Size() + uint32(4)
	if length > BlockCapacity {
		return errors.New("block 超过容量了！")
	}
	b.Length = length
	b.Datas = append(b.Datas, s)
	b.DataSize = b.DataSize + 1
	b.Offsets = append(b.Offsets, offset)
	return nil
}

// 返回block中smalldata的数组长度
func (b *Block) Size() int {
	return len(b.Offsets)
}

func (b *Block) Index(index int) SmallData {
	return b.Datas[index]
}

