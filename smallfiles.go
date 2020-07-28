package smallfiles

import (
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"os"
	"regexp"
	"strconv"
	"sync"
)

func PathExists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

type blockInfo struct {
	block Block
	blockNum int
	fileId int
}

type IterateFun func(block *Block)

type cache struct { // todo lru缓存

}

var (
	SUFFIX = ".sf"
	FORMAT = regexp.MustCompile(`([\d]+).sf`)
	METABS = uint64(12)
	LOCK sync.Mutex
)

type SmallFiles struct {
	c cache
	sf SmallDataFormat
	files map[uint8] *os.File     // fileId作为map的key值，value为File对象
	maxOffsets map[uint8] uint64  // 每个file对应的最大的offset
	blockNums map[uint8] uint32   // 每个file对应的block数量
}

func Init(directory string, sf SmallDataFormat, files int) *SmallFiles {
	fs := make(map[uint8]*os.File, 0)
	maxs := make(map[uint8] uint64, 0)
	es, err := PathExists(directory)
	if err != nil {
		return nil
	}
	if !es { // 不存在该目录，创建目录
		err := os.Mkdir(directory, os.ModePerm)
		if err != nil {
			return nil
		}
	}
	for i := 0; i < files; i ++ {
		filePath := fmt.Sprintf("%s/%d%s", directory, i, SUFFIX)
		f, err := os.Create(filePath)
		if err != nil {
			fmt.Println("init smallfiles err:", err)
			return nil
		}
		fs[uint8(i)] = f
		maxs[uint8(i)] = 0
	}

	return &SmallFiles{
		sf: sf,
		files: fs,
		maxOffsets: maxs,
	}
}

// 读取file中存储的blockNum
func maxOffset(f *os.File) uint64 {
	fileInfo, _ := f.Stat()
	if fileInfo.Size() == 0 {
		return 0
	}
	bytes := make([]byte, 8)
	_, err := f.ReadAt(bytes, 0)
	if err != nil {
		fmt.Println("maxNum, ", err)
		return 0
	}
	return binary.BigEndian.Uint64(bytes)
}

func Open(directory string,format SmallDataFormat) *SmallFiles {
	// todo check 目录下的文件是否符合规则： xx/1.sf 类型的文件名格式
	fs, err:= ioutil.ReadDir(directory)
	if err != nil {
		fmt.Println("err", err)
		return nil
	}

	s := make(map[uint8]*os.File, 0)
	maxs := make(map[uint8]uint64, 0)
	for _, f := range fs {
		fn := f.Name()
		m := FORMAT.FindStringSubmatch(fn)
		if len(m) == 2 {
			id, _ := strconv.ParseUint(m[1], 10, 32)
			fi, err := os.OpenFile(directory + f.Name(), os.O_RDWR, os.ModeAppend)
			if err != nil {
				fmt.Println("err", err)
			}
			iu := uint8(id)
			s[iu] = fi
			maxs[iu] = maxOffset(fi)
		}
	}

	return &SmallFiles{
		files:     s,
		sf: format,
		maxOffsets: maxs,
	}
}

// todo 添加缓存机制
func (s *SmallFiles) addCache(bi *blockInfo) {

}

func (s *SmallFiles) GetBlock(fileId uint8, offset int64) (*Block, error) {

	bytes := make([]byte, 64 * 1024)
	fileObj := s.files[fileId]
	_, err := fileObj.ReadAt(bytes, offset)
	if err != nil {
		return nil, err
	}

	return GenerateBlockFromBytes(bytes,s.sf), nil
}

// 根据文件偏移获取block链上所有block
func (s *SmallFiles) GetBlockArray(filedId uint8, offset int64) ([]*Block, error) {
	var blocks []*Block
	b, e := s.GetBlock(filedId, offset)
	if e != nil {
		return nil, e
	}
	blocks = append(blocks, b)
	for b.NextOffset != 0 {
		nextblock, e := s.GetBlock(filedId, int64(b.NextOffset))
		if e != nil {
			return nil, e
		}
		blocks = append(blocks, nextblock)
		b = nextblock
	}
	return blocks, nil
}

func (s *SmallFiles) flushMeta(fileId uint8) error {
	fileObj := s.files[fileId]
	offset := s.maxOffsets[fileId]
	num := s.blockNums[fileId]

	bytes := make([]byte, int(METABS))
	binary.BigEndian.PutUint64(bytes, offset)
	binary.BigEndian.PutUint32(bytes, num)

	_, err := fileObj.WriteAt(bytes, 0)
	return err
}

func (s *SmallFiles) flushBlock(b *Block, fileId uint8, offset uint64) error {
	bytes := b.Bytes()
	fileObj := s.files[fileId]
	_, err := fileObj.WriteAt(bytes, int64(offset))
	return err
}

func (s *SmallFiles) getMaxOffset(fileId uint8) uint64 {
	offset := s.maxOffsets[fileId]
	return offset
}

func (s *SmallFiles) setMaxOffset(fileId uint8, o uint64) {
	s.maxOffsets[fileId] = o
}

// 新建一block 并进行一次填充字节数据,
func (s *SmallFiles) FillBytes(bytes []byte, fileId uint8) (error, uint64) {
	block := GenerateBlockFromBytes(bytes, s.sf)
	offset := s.getMaxOffset(fileId)
	s.setMaxOffset(fileId, offset + uint64(block.Capacity))

	err := s.flushMeta(fileId)
	if err != nil {
		return err, 0
	}
	return s.flushBlock(block, fileId, offset), s.getMaxOffset(fileId)
}

// 新建一block 并进行数据fill, 返回maxOffset
func (s *SmallFiles) FillDatas(datas []SmallData, fileId uint8) (error, uint64) {
	block, _ := GenerateBlock(datas)
	offset := s.getMaxOffset(fileId)
	if offset == 0 {
		offset = METABS
	}
	s.setMaxOffset(fileId, offset + uint64(block.Capacity))
	err := s.flushMeta(fileId)
	if err != nil {
		return err, 0
	}

	return s.flushBlock(block, fileId, offset), s.getMaxOffset(fileId)
}

// 重新填充数据
func (s *SmallFiles) RefillDatas(datas []SmallData, fielId uint8, blockNum int) error {
	block, _ := GenerateBlock(datas)
	fmt.Println("blockNum", blockNum, fielId)
	return s.flushBlock(block, fielId, uint64(blockNum))
}

func (s *SmallFiles) BlocksSize() uint32 {
	var total uint32
	for _, n := range s.blockNums {
		total = total + n
	}
	return total
}

// 迭代出所有的block
func (s *SmallFiles) Iterator(fn IterateFun) {
	for fid, _ := range s.files {
		maxBn := s.getMaxOffset(fid)
		for i := 0; i < int(maxBn); i ++ {
			block, error := s.GetBlock(fid, int64(i))
			if error == nil {
				fn(block)
			}
		}
	}
}

// 往某个block 追加数据
func (s *SmallFiles) AppendBytes(bytes []byte, fileId uint8, offset int64) error {
	b, err := s.GetBlock(fileId, offset)
	if err != nil {
		return err
	}
	sd := s.sf(bytes)
	b.AddData(sd)
	e := s.flushBlock(b, fileId, uint64(offset))
	return e
}

func (s *SmallFiles) initBlock(field uint8, capacity int) (*Block, uint64){
	offset := s.getMaxOffset(field)
	s.setMaxOffset(field, offset + uint64(capacity))
	s.flushMeta(field)
	return &Block{
		Type:  chooseType(capacity),
		Capacity:   capacity,
		NextOffset: 0,
	}, offset
}

func (s *SmallFiles) AppendDatas(datas []SmallData, fileId uint8, offset uint64) error {
	LOCK.Lock()
	defer LOCK.Unlock()

	block, err := s.GetBlock(fileId, int64(offset))
	if err != nil {
		return err
	}

	for _, d := range datas {
		err := block.AddData(d)
		if err != nil { // 代表block已满，需要再新建一个block成链
			nblock, begin := s.initBlock(fileId, block.Capacity)
			block.SetNextOffset(begin)
			s.flushBlock(block, fileId, offset)
			block = nblock
			offset = begin
		}
	}
	e := s.flushBlock(block, fileId, offset)
	return e
}




