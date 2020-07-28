package smallfiles

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
)

/**
	动态大小的block：
	block开始大小：64kb(type: 1)
	block开始大小：128kb(type: 2)
	block开始大小：256kb(type: 3)
	block开始大小：512kb(type: 4)
	block开始大小：1024kb(type: 5)
	block开始大小：2048kb(type: 6)
    block开始大小：4096kb(type: 7)
    block开始大小：8192kb(type: 8)
总体实现思路：
根据一开始数据大小选择不同类型的block，如选择了256kb的block，再在上面追加数据的时候如果超出了block大小，新建一个同一大小的block和之前的
形成链，每个初始block的文件偏移会被记录
*/

var (
	BlockCapacity = 8192 * 1024
	NEXTOFFSETBS = 8
	TYPEBS = 1
	DATASIZEBS = 2
	BLOCKMETABS = TYPEBS + DATASIZEBS
	FIXEDBS = TYPEBS + DATASIZEBS + NEXTOFFSETBS
)

type SmallData interface { // 标识存储的最下数据单元，如存取的一条文章信息
	Size() int
	Serialize() []byte
}

type SmallDataFormat func(bytes []byte) SmallData

/**
- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
| type(1b) | datasize |  datas (xxxb) ....................| offsets 数组 |  nextoffset(8b) |
- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
			- - - - - - - - - - - - - - - - -
			...... | d5 | d4 | d3 | d2 | d1
			- - - -+- - + - -+- - + - -+- - -
offsets[i]:		   4    3    2    1    0
offsets 数组填写按照倒序顺序
 */
type Block struct {
	Type uint8 	  		  // 该block的类型
	Capacity int
	Offsets []int         // smalldata 对应的offset 数组
	Datas []SmallData
	NextOffset uint64
}

func sizeNeed(datas []SmallData) int {
	var total int
	for _, i := range datas {
		total = total + i.Size()
	}
	return len(datas) * 4 + total + FIXEDBS
}

func bytesSize(typ uint8) int {
	return (1 << typ) * 32 * 1024
}

func chooseType(size int) uint8 {
	s := math.Ceil(float64(size) / (32 * 1024))
	if s < 2 {
		s = 2
	}
	t := math.Ceil(math.Log2(s))
	if t >= 1 && t <= 8 {
		return uint8(t)
	}

	return 0
}

// 从零开始生成一个block
func GenerateBlock(datas []SmallData) (*Block, error) {
	begin := TYPEBS + DATASIZEBS
	var offsets []int
	need := sizeNeed(datas)
	if need > BlockCapacity {
		return nil, errors.New("超出最大范围：2mb!")
	}
	for _, d := range datas {
		begin = begin + d.Size()
		offsets = append(offsets, begin)
	}

	t := chooseType(need)
	cap := bytesSize(t)
	b := &Block{
		Type: t,
		Capacity: cap,
		Offsets:  offsets,
		Datas:    datas,
	}

	return b, nil
}

func GenerateBlockFromBytes(bytes []byte, sdf SmallDataFormat) *Block {
	ds := binary.BigEndian.Uint16(bytes[1:3])
	var offsets []int
	var datas []SmallData
	begin := len(bytes) - NEXTOFFSETBS
	nextOffset := binary.BigEndian.Uint64(bytes[begin:])

	for i:= 0; i < int(ds); i ++ {
		o := int(binary.BigEndian.Uint32(bytes[begin - i * 4 - 4 : begin - i * 4 ]))
		offsets = append(offsets, o)
	}
	if len(offsets) < 1 {
		return nil
	}

	offset := TYPEBS + DATASIZEBS
	for i, _ := range offsets {
		os := offsets[i]
		datas = append(datas, sdf(bytes[offset: os]))
		offset = os
	}

	b := &Block{
		Type: bytes[0],
		Capacity: bytesSize(bytes[0]),
		Offsets:  offsets,
		Datas:    datas,
		NextOffset: nextOffset,
	}
	return b
}

func (b *Block) Bytes() []byte {
	out := make([]byte, b.Capacity)
	out[0] = b.Type
	binary.BigEndian.PutUint16(out[1: 3], uint16(b.Size()))
	var offsets []uint32
	begin := TYPEBS + DATASIZEBS
	for i:=0; i < len(b.Datas); i ++ {
		ds := b.Datas[i].Serialize()
		if begin + len(ds) > 65536 {
			fmt.Println("nodood")
		}
		copy(out[begin: begin + len(ds)], ds)
		begin = begin + len(ds)
		offsets = append(offsets, uint32(begin))
		obegin := b.Capacity - NEXTOFFSETBS - i * 4 - 4
		binary.BigEndian.PutUint32(out[obegin : obegin + 4 ], uint32(begin))

	}

	binary.BigEndian.PutUint64(out[b.Capacity - 8:], b.NextOffset)
	return out
}

// 返回block剩余的byte数
func (b *Block) Left() int {
	lastoffset := 0
	if len(b.Offsets) > 0 {
		lastoffset = b.Offsets[len(b.Offsets) - 1]
	}
	return b.Capacity - lastoffset - NEXTOFFSETBS - len(b.Datas) * 4
}

// 追加的时候，如果block超出capacity，就进行添加新block，追加链
func (b *Block) AddData(s SmallData) error{
	size := s.Size()
	length := size + 4
	if length > b.Left() {
		return errors.New("block 超过容量了！")
	}
	b.Datas = append(b.Datas, s)
	lastOffset := BLOCKMETABS
	if len(b.Offsets) > 0 {
		lastOffset = b.Offsets[len(b.Offsets) - 1]
	}
	b.Offsets = append(b.Offsets, lastOffset + size)
	return nil
}

// 返回block中smalldata的数组长度
func (b *Block) Size() int {
	return len(b.Datas)
}

func (b *Block) Index(index int) SmallData {
	return b.Datas[index]
}

func (b *Block) SetNextOffset(offset uint64) {
	b.NextOffset = offset
}

