package consumers

import (
	"cache-updater/cacher"
	"cache-updater/db"
	"crypto/tls"
	"github.com/Gravity-Hub-Org/gravity-node-api-mockup/v2/model"
	"github.com/go-pg/pg/v10"
	"github.com/joho/godotenv"
	"os"
	"time"
)

type PGDBConsumer struct {
	DestinationDB *pg.DB
	ConsumerDB *pg.DB
	NebulaeMap map[cacher.CacherType][]string

	consumerDBHelper *db.DBHelper
	timeout time.Duration
}

func (c *PGDBConsumer) consume () {
	nebulas, _ := c.consumerDBHelper.Nebulae()
	nodes, _ := c.consumerDBHelper.Nodes()

	for _, nebula := range nebulas {
		searchNebula := new(model.Nebula)
		c.DestinationDB.Model(searchNebula).Where("address = ?", nebula.Address).Select()

		if searchNebula.Address == "" {
			err := c.DestinationDB.Insert(&nebula)
			if err != nil {
				println(err.Error())
			}
		} else {
			_, err := c.DestinationDB.Model(searchNebula).Where("address = ?", nebula.Address).Update(&nebula)
			if err != nil {
				println(err.Error())
			}
		}
	}

	for _, node := range nodes {
		searchNode := new(model.Node)
		c.DestinationDB.Model(searchNode).Where("address = ?", node.Address).Select()

		if searchNode.Address == "" {
			err := c.DestinationDB.Insert(&node)
			if err != nil {
				println(err.Error())
			}
		} else {
			_, err := c.DestinationDB.Model(searchNode).Where("address = ?", node.Address).Update(&node)
			if err != nil {
				println(err.Error())
			}
		}
	}

}

func (c *PGDBConsumer) startConsume () {
	c.consume()
	
	time.AfterFunc(3*time.Second, c.startConsume)
}

func (c *PGDBConsumer) Start(nebulaeMap map[cacher.CacherType][]string) {
	c.DestinationDB = c.ConnectToPG()
	
	c.consumerDBHelper = &db.DBHelper{
		Db:         c.ConsumerDB,
		NebulaeMap: nebulaeMap,
	}
	c.timeout = 3 * time.Second
	c.startConsume()
}

func (c *PGDBConsumer) GetDBCredentials () (string, string, string, string, string) {
	envLoadErr := godotenv.Load(".env")
	if envLoadErr != nil {
		_ = godotenv.Load(".env.example")
	}

	dbhost := "localhost"
	dbport := "5432"
	if os.Getenv("DB_HOST") != "" {
		dbhost = os.Getenv("DB_HOST")
	}
	if os.Getenv("DB_PORT") != "" {
		dbport = os.Getenv("DB_PORT")
	}
	dbuser := os.Getenv("DB_USERNAME")
	dbpass := os.Getenv("DB_PASS")
	dbdatabase := os.Getenv("DB_NAME")

	return dbhost, dbport, dbuser, dbpass, dbdatabase
}

func (c *PGDBConsumer) ConnectToPG () *pg.DB {
	dbhost, dbport, dbuser, dbpass, dbdatabase := c.GetDBCredentials()

	db := pg.Connect(&pg.Options{
		Addr: dbhost + ":" + dbport,
		User:     dbuser,
		Password: dbpass,
		Database: dbdatabase,
		TLSConfig: &tls.Config{
			InsecureSkipVerify: true,
		},
	})
	return db
}