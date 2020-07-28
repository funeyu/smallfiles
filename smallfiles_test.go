package smallfiles

import (
	"fmt"
	"strings"
	"testing"
)

// article 落盘的格式为： {id}##{title}##{favicon}##{desc}
type Article struct {
	id string
	title string
	favicon string
	desc string
}


func (a *Article) Size() int {
	total := len(a.id) + len(a.title) + len(a.favicon) + len(a.desc) + 6
	return total
}

func (a *Article) Serialize() []byte {
	str := strings.Join([]string {a.id, a.title, a.favicon, a.desc}, "##")
	var bytes []byte = []byte(str)
	return bytes
}

func TestSmallFiles_AppendDatas(t *testing.T) {
	s := Init("./test", func(bytes []byte) SmallData {
		str := string(bytes)
		ss := strings.Split(str, "##")
		art := &Article{
			id:      ss[0],
			title:   ss[1],
			favicon: ss[2],
			desc:    ss[3],
		}
		return art
	}, 1)

	art := &Article{
		id:      "12",
		title:   "测试title",
		favicon: "测试favicon图标",
		desc:    "测试描述",
	}
	art1 := &Article{
		id:      "13",
		title:   "测试一把",
		favicon: "测试fff",
		desc:    "desc",
	}
	s.FillDatas([]SmallData{art, art1}, 0)
}

func TestSmallFiles_GetBlock(t *testing.T) {
	s := Open("./test/", func(bytes []byte) SmallData {
		str := string(bytes)
		ss := strings.Split(str, "##")
		art := &Article{
			id:      ss[0],
			title:   ss[1],
			favicon: ss[2],
			desc:    ss[3],
		}
		return art
	})

	var ss []SmallData
	for i:=0; i < 500 ; i ++ {
		a := &Article{
			id:      "132323323332",
			title:   "测试一把title再长一些好不好好不好好不好好不好好不好好不好好不好测试一把title再长一些好不好好不好好不好好不好好不好好不好好不好",
			favicon: "测试fff",
			desc:    "desc",
		}
		ss = append(ss, a)
	}
	s.AppendDatas(ss, 0, METABS)
}

func TestOpen(t *testing.T) {
	s := Open("./test/", func(bytes []byte) SmallData {
		str := string(bytes)
		ss := strings.Split(str, "##")
		art := &Article{
			id:      ss[0],
			title:   ss[1],
			favicon: ss[2],
			desc:    ss[3],
		}
		return art
	})

	bs, _ := s.GetBlockArray(0, int64(METABS))
	total := 0
	for _, d := range bs {
		for _, da := range d.Datas {
			fmt.Println("b", da)
			total = total + 1
		}
	}
	fmt.Println("total", total)
}



