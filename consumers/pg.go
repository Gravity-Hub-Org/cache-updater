package consumers

import (
	"cache-updater/cacher"
	"cache-updater/db"
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

	_ = c.DestinationDB.Insert(nebulas)
	_ = c.DestinationDB.Insert(nodes)
}

func (c *PGDBConsumer) startConsume () {
	c.consume()
	
	time.AfterFunc(3*time.Second, c.startConsume)
}

func (c *PGDBConsumer) Start() {
	c.DestinationDB = c.ConnectToPG()
	
	c.consumerDBHelper = &db.DBHelper{
		Db:         c.ConsumerDB,
		NebulaeMap: nil,
	}
	c.timeout = 3 * time.Second
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
	})
	return db
}