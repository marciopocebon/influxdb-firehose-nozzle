package cfinstanceinfoapi

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"net/http"
	"time"

	"github.com/evoila/influxdb-firehose-nozzle/nozzleconfig"
)

type AppInfo struct {
	Name  string `json:"name,omitempty"`
	Guid  string `json:"guid,omitempty"`
	Space string `json:"space,omitempty"`
	Org   string `json:"org,omitempty"`
}

func UpdateAppMap(config *nozzleconfig.NozzleConfig, appmap map[string]AppInfo) {
	
	c := time.Tick(3 * time.Minute)
	for _ = range c {
		GenAppMap(config, appmap)
	}
}

func GenAppMap(config *nozzleconfig.NozzleConfig, appmap map[string]AppInfo) {
	log.Println("updating app map")

	pres, err := http.Get(config.AppInfoApiUrl)
	if err != nil {
		log.Fatal(err)
	}

	pbody, err := ioutil.ReadAll(pres.Body)
	pres.Body.Close()
	if err != nil {
		log.Fatal(err)
	}

	var pinfo []AppInfo
	err = json.Unmarshal(pbody, &pinfo)

	for index := range pinfo {
		appmap[pinfo[index].Guid] = pinfo[index]
	}
}
