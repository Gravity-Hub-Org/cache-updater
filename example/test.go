package example

import (
	"cache-updater/cacher"
	dbHelper "cache-updater/db"

	"github.com/go-pg/pg/v10"
)

func Handle(database *pg.DB, NebulaeMap map[cacher.CacherType][]string) {
	helper := dbHelper.DBHelper{
		Db:         database,
		NebulaeMap: NebulaeMap,
	}
	consuls, err := helper.Nebulae()
	if err != nil {
		println(err.Error())
	}
	for _, consul := range consuls {
		println(consul.Name)
	}

	status, err := helper.CommonStatus()
	if err != nil {
		println(err.Error())
	}
	println(status.NodesCount)

	nodes, err := helper.Nodes()
	if err != nil {
		println(err.Error())
	}
	println(nodes)
}
