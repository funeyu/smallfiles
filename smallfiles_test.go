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

func (a *Article) Size() uint32 {
	total := len(a.id) + len(a.title) + len(a.favicon) + len(a.desc) + 6
	return uint32(total)
}

func (a *Article) Serialize() []byte {
	str := strings.Join([]string {a.id, a.title, a.favicon, a.desc}, "##")
	var bytes []byte = []byte(str)
	return bytes
}


func TestInit(t *testing.T) {
	sf := Init("./test/", func(bytes []byte) SmallData {
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
	sf.FillDatas([]SmallData {art, art1}, 0)
}

func TestSmallFiles_RefillDatas(t *testing.T) {
	sf := Open("./test/", func(bytes []byte) SmallData {
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
	art := &Article{
		id:      "1221",
		title:   "测试title",
		favicon: "测试favicon图标",
		desc:    "测试描述",
	}
	art1 := &Article{
		id:      "1331",
		title:   "测试一把",
		favicon: "测试fff",
		desc:    "desc",
	}
	sf.RefillDatas([]SmallData {art, art1}, 0, 1)
	data, _ := sf.GetBlock(0, 1)
	for _, d := range data.Datas {
		art := d.(*Article)
		fmt.Println("data:", art)
	}
}


