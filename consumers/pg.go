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

// swagger:model
type DataFeeds struct {
	tableName struct{} `sql:"data_feeds"`

	// Data feed tag for distinct usage
	//
	// required: true
	DataFeedTag string `json:"datafeed_tag" pg:"data_feed_tag"`

	// Common extractor description
	//
	// required: true
	Description string `json:"description"`
}

func (c *PGDBConsumer) consume () error {
	nebulas, err := c.consumerDBHelper.Nebulae()
	if err != nil {
		return err
	}

	nodes, err := c.consumerDBHelper.Nodes()
	if err != nil {
		return err
	}

	pulses, nodeCount, err := c.consumerDBHelper.CommonStatus()
	if err != nil {
		return err
	}

	for _, nebula := range nebulas {
		_, err := c.DestinationDB.Model(new(model.Nebula)).Where("address = ?", nebula.Address).Delete()
		if err != nil {
			return err
		}

		err = c.DestinationDB.Insert(&nebula)
		if err != nil {
			return err
		}
	}

	for _, node := range nodes {
		_, err := c.DestinationDB.Model(new(model.Node)).Where("address = ?", node.Address).Delete()
		if err != nil {
			return err
		}

		err = c.DestinationDB.Insert(&node)
		if err != nil {
			return err
		}
	}

	_, err = c.DestinationDB.Model(new(model.CommonStats)).Where("true").ForceDelete()
	if err != nil {
		return err
	}

	dataFeedsCount, err := c.DestinationDB.Model(new(DataFeeds)).Count()
	if err != nil {
		return err
	}

	err = c.DestinationDB.Insert(&model.CommonStats{
		NodesCount: nodeCount,
		Pulses:     pulses,
		DataFeeds:  uint(dataFeedsCount),
	})
	if err != nil {
		return err
	}

	return nil

}

func (c *PGDBConsumer) startConsume () {
	err := c.consume()
	if err != nil {
		println(err.Error())
	}
	
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