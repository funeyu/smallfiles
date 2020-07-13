package smallfiles

import (
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"os"
	"regexp"
	"smallfiles/util"
	"strconv"
)

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
)

type SmallFiles struct {
	c cache
	sf SmallDataFormat
	files map[uint8] *os.File     // fileId作为map的key值，value为File对象
	maxBlockNums map[uint8] uint32 // 每个file对应的最大blockNum
}

func Init(directory string, sf SmallDataFormat, cap int) *SmallFiles {
	fs := make(map[uint8]*os.File, 0)
	maxNums := make(map[uint8] uint32, 0)
	es, err := util.PathExists(directory)
	if err != nil {
		return nil
	}
	if !es { // 不存在该目录，创建目录
		err := os.Mkdir(directory, os.ModePerm)
		if err != nil {
			return nil
		}
	}
	for i := 0; i < cap; i ++ {
		filePath := fmt.Sprintf("%s/%d%s", directory, i, SUFFIX)
		f, err := os.Create(filePath)
		if err != nil {
			fmt.Println("init smallfiles err:", err)
			return nil
		}
		fs[uint8(i)] = f
		maxNums[uint8(i)] = 0
	}

	return &SmallFiles{
		sf: sf,
		files: fs,
		maxBlockNums: maxNums,
	}
}

// 读取file中存储的blockNum
func maxNum(f *os.File) uint32 {
	fileInfo, _ := f.Stat()
	if fileInfo.Size() == 0 {
		return 0
	}
	bytes := make([]byte, 4)
	_, err := f.ReadAt(bytes, 0)
	if err != nil {
		fmt.Println("maxNum, ", err)
		return 0
	}
	return binary.BigEndian.Uint32(bytes)
}

func Open(directory string,format SmallDataFormat) *SmallFiles {
	// todo check 目录下的文件是否符合规则： xx/1.sf 类型的文件名格式
	fs, err:= ioutil.ReadDir(directory)
	if err != nil {
		fmt.Println("err", err)
		return nil
	}

	s := make(map[uint8]*os.File, 0)
	maxNums := make(map[uint8]uint32, 0)
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
			maxNums[iu] = maxNum(fi)
		}
	}

	return &SmallFiles{
		files:     s,
		sf: format,
		maxBlockNums: maxNums,
	}
}

// todo 添加缓存机制
func (s *SmallFiles) addCache(bi *blockInfo) {

}

func (s *SmallFiles) GetBlock(fileId uint8, blockNum int) (*Block, error) {
	bytes := make([]byte, BlockCapacity)
	fileObj := s.files[fileId]
	_, err := fileObj.ReadAt(bytes, 4 + int64(blockNum) * BlockCapacity)
	if err != nil {
		return nil, err
	}

	return GenerateBlockFromBytes(bytes,s.sf), nil
}

func (s *SmallFiles) flushNum(fileId uint8) error {
	fileObj := s.files[fileId]
	num := s.maxBlockNums[fileId]
	bytes := make([]byte, 4)
	binary.BigEndian.PutUint32(bytes, num)

	_, err := fileObj.WriteAt(bytes, 0)
	return err
}

func (s *SmallFiles) flushBlock(b *Block, fileId uint8, blockNum int) error {
	bytes := b.Bytes()
	fileObj := s.files[fileId]
	_, err := fileObj.WriteAt(bytes, 4 + int64(blockNum) * BlockCapacity)
	return err
}

func (s *SmallFiles) maxBlockNum(fileId uint8) uint32 {
	return s.maxBlockNums[fileId]
}

// 新建一block 并进行一次填充字节数据,
func (s *SmallFiles) FillBytes(bytes []byte, fileId uint8) (error, uint32) {
	num := s.maxBlockNum(fileId)
	s.maxBlockNums[fileId] = num + 1
	err := s.flushNum(fileId)
	if err != nil {
		return err, 0
	}

	block := GenerateBlockFromBytes(bytes, s.sf)
	return s.flushBlock(block, fileId, int(num + 1)), s.maxBlockNums[fileId]
}

// 新建一block 并进行数据fill, 返回maxBlockNums
func (s *SmallFiles) FillDatas(datas []SmallData, fileId uint8) (error, uint32) {
	num := s.maxBlockNums[fileId]
	s.maxBlockNums[fileId] = num + 1
	err := s.flushNum(fileId)
	if err != nil {
		return err, 0
	}

	block := GenerateBlock(datas)
	return s.flushBlock(block, fileId, int(num + 1)), s.maxBlockNum(fileId)
}

// 重新填充数据
func (s *SmallFiles) RefillDatas(datas []SmallData, fielId uint8, blockNum int) error {
	block := GenerateBlock(datas)
	fmt.Println("blockNum", blockNum, fielId)
	return s.flushBlock(block, fielId, blockNum)
}

func (s *SmallFiles) BlocksSize() uint32 {
	var total uint32
	for _, n := range s.maxBlockNums {
		total = total + n
	}
	return total
}

// 迭代出所有的block
func (s *SmallFiles) Iterator(fn IterateFun) {
	for fid, _ := range s.files {
		maxBn := s.maxBlockNums[fid]
		for i := 0; i < int(maxBn); i ++ {
			block, error := s.GetBlock(fid, i)
			if error == nil {
				fn(block)
			}
		}
	}
}

// 往某个block 追加数据
func (s *SmallFiles) AppendBytes(bytes []byte, fileId uint8, blockNum int) error {
	b, err := s.GetBlock(fileId, blockNum)
	if err != nil {
		return err
	}
	sd := s.sf(bytes)
	b.AddData(sd)
	e := s.flushBlock(b, fileId, blockNum)
	return e
}

func (s *SmallFiles) AppendDatas(datas []SmallData, fileId uint8, blockNum int) error {
	block, err := s.GetBlock(fileId, blockNum)
	if err != nil {
		return err
	}

	for _, d := range datas {
		block.AddData(d)
	}
	e := s.flushBlock(block, fileId, blockNum)
	return e
}




