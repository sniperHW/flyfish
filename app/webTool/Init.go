package webTool

import (
	"fmt"
	"github.com/sniperHW/flyfish/app/webTool/conf"
	"github.com/sniperHW/flyfish/client"
	"github.com/sniperHW/kendynet/golog"
	"github.com/yddeng/flyfish/app/webTool/db"
	"net/http"
	"strings"
	"time"
)

var (
	logger     *golog.Logger
	flyClients = map[string]*FlyfishClient{}
	timeout    = time.Hour * 1
)

type FlyfishClient struct {
	*client.Client
	deadline time.Time
}

func GetFlyClient(flyConfig string) (*FlyfishClient, error) {
	now := time.Now()

	c, ok := flyClients[flyConfig]
	if ok {
		c.deadline = now.Add(timeout)
		return c, nil
	}

	s := strings.Split(flyConfig, "@")
	if len(s) != 2 {
		return nil, fmt.Errorf("%s is failed", flyConfig)
	}

	client := client.OpenClient(fmt.Sprintf("%s:%s", s[0], s[1]), false)

	flyClients[flyConfig] = &FlyfishClient{
		Client:   client,
		deadline: now.Add(timeout),
	}
	return c, nil
}

func Init(config *conf.Config) error {
	outLogger := golog.NewOutputLogger("log", "flyfish_tool", 1024*1024*50)
	logger = golog.New("flyfish_tool", outLogger)
	logger.Debugf("%s logger init", "flyfish_tool")

	http.Handle("/", http.StripPrefix("/", http.FileServer(http.Dir(config.LoadDir))))
	logger.Infof("http start on %s, LoadDir on %s\n", config.HttpAddr, config.LoadDir)

	http.HandleFunc("/testConnection", HandleTestConnection)
	http.HandleFunc("/createTable", HandleCreateTable)
	http.HandleFunc("/tableInfo", HandleTableInfo)
	http.HandleFunc("/addColumn", HandleAddColumn)
	http.HandleFunc("/set", HandleSet)
	http.HandleFunc("/get", HandleGet)
	http.HandleFunc("/del", HandleDel)
	//http.HandleFunc("/truncate", HandleTruncate)
	err := http.ListenAndServe(config.HttpAddr, nil)

	go Loop()
	return err
}

func Loop() {
	timer := time.NewTicker(time.Minute)
	for {
		now := <-timer.C
		db.Tick(now)
		for k, c := range flyClients {
			if now.After(c.deadline) {
				delete(flyClients, k)
			}
		}
	}
}
